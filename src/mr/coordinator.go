package mr

import (
	"container/list"
	"errors"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	lock            sync.Mutex
	NReduce         int
	MapTaskQueue    *list.List // 新的任务
	ReduceTaskQueue *list.List

	AssignedMapTaskMap    map[int]*MapTask // 刚分配给worker但还未被完成的任务，value是task指针
	AssignedReduceTaskMap map[int]*ReduceTask

	mapNum    int32 // 可被分配的map任务数量，当worker反馈任务完成时，才能进行减1操作
	reduceNum int32
}

const (
	MAXWAITTIME  = 10
	MAPTYPE      = 0
	REDUCETYPE   = 1
	onceWaitTime = time.Second // todo for test
	//onceWaitTime = time.Millisecond
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *MRArgs, reply *MRReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	reply.NReduce = c.NReduce
	if c.mapNum != 0 {
		if c.MapTaskQueue.Len() != 0 {
			task := c.MapTaskQueue.Front().Value.(MapTask)
			reply.MapTask = &task
			c.MapTaskQueue.Remove(c.MapTaskQueue.Front())
			c.AssignedMapTaskMap[task.Id] = &task
			return nil
		} else if len(c.AssignedMapTaskMap) != 0 {
			task := c.ReassignTask(MAPTYPE)
			if task != nil {
				reply.MapTask = task.(*MapTask)
				return nil
			}
		}
	} else if c.reduceNum != 0 {
		// If the code can be executed here, it means the all map task finished
		if c.ReduceTaskQueue.Len() != 0 {
			task := c.ReduceTaskQueue.Front().Value.(ReduceTask)
			reply.ReduceTask = &task
			c.ReduceTaskQueue.Remove(c.ReduceTaskQueue.Front())
			c.AssignedReduceTaskMap[task.Id] = &task
			return nil
		} else if len(c.AssignedReduceTaskMap) != 0 {
			task := c.ReassignTask(REDUCETYPE)
			if task != nil {
				reply.ReduceTask = task.(*ReduceTask)
				return nil
			}
		}
	}
	return errors.New("warn: all tasks are assigned")
}

// FinishTask called by worker after finished a task
/*
 * FinishTask may be executed concurrently with ReassignTask or AssignTask,
 * but it won't cause thread-unsafe, so don't need lock
 */
func (c *Coordinator) FinishTask(args *MRArgs, reply *MRReply) error {
	if args.MapTask != nil {
		task, ok := c.AssignedMapTaskMap[args.MapTask.Id]
		if ok && task != nil {
			task.Status = FINISHED
			args.MapTask.Status = FINISHED
			reply.MapTask = args.MapTask
			/**
			AssignTask方法进行了加锁处理，所以同一个时刻，每个Worker分配的task必不相同,
			如此，不会造成mapNum/reduceNum的重复减1
			*/
			atomic.AddInt32(&c.mapNum, -1)
			if c.mapNum == 0 {
				log.Printf("info: all map tasks are assigned!")
			}

		} //} else {
		//	return errors.New("error: fail to finish map task with id=" + strconv.Itoa(args.MapTask.Id))
		//}
	} else if args.ReduceTask != nil {
		task, ok := c.AssignedReduceTaskMap[args.ReduceTask.Id]
		if ok {
			task.Status = FINISHED
			args.ReduceTask.Status = FINISHED
			reply.ReduceTask = args.ReduceTask
			atomic.AddInt32(&c.reduceNum, -1)
			//log.Printf("info: the reduce task with id = %d is finished by worker\n", task.Id)
		}
		//} else {
		//	return errors.New("error: fail to finish reduce task with id=" + strconv.Itoa(task.Id))
		//}
	}
	// for now,
	return nil
}

// ReassignTask Check whether there are any unfinished tasks, and reissue it to other worker
func (c *Coordinator) ReassignTask(taskType int) interface{} {
	switch taskType {
	case MAPTYPE:
		{
			for k, v := range c.AssignedMapTaskMap {
				/* 一个任务如果超过 MAXWAITTIME 秒没有被反馈已完成（通过worker调用FinishTask()）
				 * 则视为该任务被分配的worker宕机，则该任务会被分配给其它worker
				 */
				j := 0
				for ; j < MAXWAITTIME; j++ {

					if v.Status == FINISHED {
						delete(c.AssignedMapTaskMap, k)
						break // 该任务已被worker标记完成
					}
					// Waiting the worker who hold this task to finish it
					time.Sleep(onceWaitTime)
				}
				if j != MAXWAITTIME {
					continue
				}
				return v
			}
		}
	case REDUCETYPE:
		{
			for k, v := range c.AssignedReduceTaskMap {
				/* 一个任务如果超过 MAXWAITTIME 秒没有被反馈已完成（通过worker调用FinishTask()）
				 * 则视为该任务被分配的worker宕机，则该任务会被分配给其它worker
				 */
				j := 0
				for ; j < MAXWAITTIME; j++ {
					// 这里的循环其实就是在等待worker调用FinishTask方法
					if v.Status == FINISHED {
						delete(c.AssignedReduceTaskMap, k)
						break
					}
					time.Sleep(onceWaitTime)
				}
				if j != MAXWAITTIME {
					continue
				} else {
					return v
				}
			}
		}
	}

	return nil

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.mapNum == 0 && c.reduceNum == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.lock = sync.Mutex{}
	c.MapTaskQueue, c.ReduceTaskQueue = list.New(), list.New()
	c.AssignedMapTaskMap = make(map[int]*MapTask)
	c.AssignedReduceTaskMap = make(map[int]*ReduceTask)
	c.NReduce = nReduce
	c.mapNum = int32(len(files))
	c.reduceNum = int32(nReduce)
	// Your code here.
	// 1. Create map tasks and reduce tasks
	for i, file := range files {
		mapTask := MapTask{i, file, INIT}
		c.MapTaskQueue.PushBack(mapTask)
	}
	for i := 0; i < nReduce; i++ {
		reduceTask := ReduceTask{i, "mr-out-" + strconv.Itoa(i), INIT}
		c.ReduceTaskQueue.PushBack(reduceTask)
	}
	c.server()
	return &c
}
