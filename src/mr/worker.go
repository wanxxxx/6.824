package mr

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"
)
import "log"
import "hash/fnv"

// Map functions return a slice of KeyValue.
//
// for sorting by key.
// for sorting by key.
type ByKey []KeyValue

//const onceWaitTime = time.Second // todo for test

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
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
	// Your worker implementation here.
	args := &MRArgs{}
	// 循环请求任务，并执行，每次请求间隔1s
	var err error
	for {
		reply := &MRReply{}
		// Call for a task from coordinator
		for i := 0; i < MAXWAITTIME; i++ {
			err = CallCoordinator(args, reply, "AssignTask")
			if err == nil {
				break
			}
			time.Sleep(onceWaitTime)
		}
		if err != nil {
			log.Printf("Exiting worker...\n")
			return
		}
		// Carry out a task
		if reply.MapTask != nil {
			err = reply.MapTask.DoMapTask(mapf, reply.NReduce)
		} else if reply.ReduceTask != nil {
			err = reply.ReduceTask.DoReduceTask(reducef)
		}
		if err != nil {
			log.Fatal(err)
			return
		}
		// Notify the coordinator that the task has been finished
		for i := 0; i < MAXWAITTIME; i++ {
			args.MapTask, args.ReduceTask = reply.MapTask, reply.ReduceTask
			err = CallCoordinator(args, reply, "FinishTask")
			if err == nil {
				if reply.MapTask != nil {
					log.Printf("success: the map task is finished, filename is %v\n", reply.MapTask.InputFilename)
				} else if reply.ReduceTask != nil {
					log.Printf("success: the reduce task is finished, id is %v\n", reply.ReduceTask.Id)
				}
				break
			}
			time.Sleep(onceWaitTime)
		}
		if err != nil {
			log.Printf("Exiting worker, because it had exceeded retry count of 'FinishTask'! %v", err)
			return
		}
		time.Sleep(onceWaitTime)
	}

}

func CallCoordinator(args *MRArgs, reply *MRReply, funcName string) error {
	ok := call("Coordinator."+funcName, &args, &reply)
	if !ok {
		switch funcName {
		case "AssignTask":
			return errors.New("error: fail to call for task from coordinator, because the coordinator is exit or all tasks are assigned")
		case "FinishTask":
			return errors.New("error: fail to notify the coordinator that the task is finished")
		}
	}
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
