package mr

import (
	"math/rand"
	"net/rpc"
	"strconv"
	"time"
)
import "log"
import "hash/fnv"

// Map functions return a slice of KeyValue.
//
// for sorting by key.
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}
type worker struct {
	workerId int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
	nReduce  int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	rand.Seed(time.Now().UnixNano())
	w := worker{
		workerId: int(rand.Int31()),
		//workerId: int(time.Now().UnixNano()),
		mapf:    mapf,
		reducef: reducef,
	}

	// Your worker implementation here.
	w.start()

}

func (w *worker) doTask(task *MrTask) {
	var err error
	switch task.TaskType {
	case MAPTYPE:
		err = task.DoMapTask(w.mapf, w.nReduce)
	case REDUCETYPE:
		err = task.DoReduceTask(w.reducef)
	default:
		panic("error: the task type is wrong")
	}
	if err != nil {
		panic(err)
	}
}

/**
 * Notify the coordinator that the task has been finished, and
 * the looping make sure the task can be finished.
 */
func (w *worker) finishTask(task *MrTask) {
	args := &MRArgs{task}
	reply := &MRReply{}
	for i := 0; i < MAXWAITTIME; i++ {
		ok := call("Coordinator.FinishTask", &args, &reply)
		if ok && reply.Task != nil && reply.Task.Status == FINISHED {
			log.Printf("finish task(type=%d, filename=%v) by worker(id=%d)", task.TaskType, task.Filename, w.workerId)
			return
		}
		time.Sleep(OnceWaitTime)
	}
	panic("error: fail to finish task(type=" + strconv.Itoa(task.TaskType) + ", filename=" + task.Filename + ")")

}
func (w *worker) requestTask() *MrTask {
	args := &MRArgs{}
	reply := &MRReply{}
	// Looping for calling a task util success
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		log.Printf("warn: fail to request task because the coordinator done")
		return nil
	} else if reply.Task == nil {
		panic("warn: reply.task is null")
		return nil
	}
	w.nReduce = reply.NReduce
	return reply.Task
}

func (w *worker) start() {
	defer func() { log.Printf("Exiting worker(id=%d)...", w.workerId) }()
	for {
		task := w.requestTask()
		if task == nil {
			return
		}
		w.doTask(task)
		/**
		 * Worker is stateless. T
		 * The coordinator won't check the worker alive, it only cares about the task
		 */
		w.finishTask(task)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
