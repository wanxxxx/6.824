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

	AssignedMapTaskArr    map[int]*MrTask // 刚分配给worker但还未被完成的任务，value是task指针
	AssignedReduceTaskArr map[int]*MrTask

	MapNum    int32 // 可被分配的map任务数量，当worker反馈任务完成时，才能进行减1操作
	ReduceNum int32
}

const (
	MAXWAITTIME  = 10
	OnceWaitTime = time.Second
	//OnceWaitTime = time.Millisecond
	Normal  = 0
	Failure = 1
	Done    = 2
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *MRArgs, reply *MRReply) error {
	reply.NReduce = c.NReduce
	var task *MrTask
	if c.MapNum != 0 {
		task = c.getTargetTypeTask(MAPTYPE)
	}
	if task == nil && c.ReduceNum != 0 {
		task = c.getTargetTypeTask(REDUCETYPE)
	}

	if task == nil {
		return errors.New("warn: all tasks are assigned")
	}
	reply.Task = task
	return nil
}

func (c *Coordinator) getTargetTypeTask(taskType int) *MrTask {
	c.lock.Lock()
	defer c.lock.Unlock()
	var taskQueue *list.List
	var taskMap *map[int]*MrTask
	switch taskType {
	case MAPTYPE:
		taskQueue, taskMap = c.MapTaskQueue, &c.AssignedMapTaskArr
	case REDUCETYPE:
		taskQueue, taskMap = c.ReduceTaskQueue, &c.AssignedReduceTaskArr
	default:
		panic("error: the task type is wrong")
	}

	task := c.getTaskFromWaitQueue(taskQueue)
	if task != nil {
		(*taskMap)[task.Id] = task
		return task
	}
	return c.reassignTask(taskMap)
}
func (c *Coordinator) getTaskFromWaitQueue(taskQueue *list.List) *MrTask {

	if taskQueue.Len() == 0 {
		return nil
	}
	task := taskQueue.Front().Value.(MrTask)
	taskQueue.Remove(taskQueue.Front())

	return &task
}

// reassignTask Check whether there are any unfinished tasks, and reissue it to other worker
func (c *Coordinator) reassignTask(taskMap *map[int]*MrTask) *MrTask {
	if len(*taskMap) == 0 {
		return nil
	}
	for _, v := range *taskMap {
		/* 一个task如果超过10秒没有被反馈已完成（通过worker调用FinishTask()）
		 * 则视为该任务被分配的worker宕机，则该任务会被分配给其它worker
		 */
		j := 0
		for ; j < MAXWAITTIME; j++ {
			if v.Status == FINISHED {
				break // 该任务已被worker标记完成
			}
			// Waiting the worker who hold this task to finish it
			time.Sleep(OnceWaitTime)
		}
		if j != MAXWAITTIME {
			continue
		}
		return v
	}
	return nil

}

// FinishTask called by worker after finished a task
/*
 * FinishTask may be executed concurrently with reassignTask or AssignTask,
 * but it won't cause thread-unsafe, so don't need lock
 */
func (c *Coordinator) FinishTask(args *MRArgs, reply *MRReply) error {
	if args.Task == nil {
		return errors.New("args.task is null")
	}
	var taskMap *map[int]*MrTask
	var num *int32
	switch args.Task.TaskType {
	case MAPTYPE:
		taskMap, num = &c.AssignedMapTaskArr, &c.MapNum
	case REDUCETYPE:
		taskMap, num = &c.AssignedReduceTaskArr, &c.ReduceNum
	default:
		panic("error: the task type is wrong")
	}

	task, ok := (*taskMap)[args.Task.Id]
	if ok && task != nil {
		task.Status = FINISHED
		args.Task.Status = FINISHED
		reply.Task = args.Task
		/**
		AssignTask方法进行了加锁处理，所以同一个时刻，每个Worker分配的task必不相同,
		如此，不会造成mapNum/reduceNum的重复减1
		*/
		atomic.AddInt32(num, -1)

		if c.MapNum == 0 {
			log.Printf("info: all map tasks are assigned!")
		}
	} else {
		return errors.New("fail to finish task, because it didn't exist")
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
	return c.MapNum == 0 && c.ReduceNum == 0
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var c = Coordinator{
		lock:                  sync.Mutex{},
		MapTaskQueue:          list.New(),
		ReduceTaskQueue:       list.New(),
		AssignedMapTaskArr:    make(map[int]*MrTask),
		AssignedReduceTaskArr: make(map[int]*MrTask),
		NReduce:               nReduce,
		MapNum:                int32(len(files)),
		ReduceNum:             int32(nReduce),
	} // Your code here.
	// 1. Create map tasks and reduce tasks
	for i, file := range files {
		mapTask := MrTask{MAPTYPE, i, file, INIT}
		c.MapTaskQueue.PushBack(mapTask)
	}
	for i := 0; i < nReduce; i++ {
		reduceTask := MrTask{REDUCETYPE, i, "mr-out-" + strconv.Itoa(i), INIT}
		c.ReduceTaskQueue.PushBack(reduceTask)
	}
	c.server()
	return &c
}
