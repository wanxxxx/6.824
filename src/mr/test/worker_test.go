package test

import (
	"6.824/mr"
	"log"
	"sync"
	"testing"
	"time"
)

func TestConcurrentWorker(t *testing.T) {
	defer func() { log.Printf("%d %d", c.MapNum, c.ReduceNum) }()
	wg := sync.WaitGroup{}
	parallel := 10
	wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go WorkerFunc(&wg, i)
	}
	wg.Wait()
	if c.Done() {
		log.Printf("Success: TestConcurrentWorker")
	} else {
		log.Fatal("Fail: TestConcurrentWorker")
	}
}

func WorkerFunc(wc *sync.WaitGroup, i int) {
	time.Sleep(time.Millisecond)
	mr.Worker(mapf, reducef)
	wc.Done()
}
func TestWorker(t *testing.T) {
	mr.Worker(mapf, reducef)
	if c.Done() {
		log.Printf("Success: TestWorker")
	} else {
		log.Fatal("Fail: TestWorker")
	}
}

func TestCallFinishTask(t *testing.T) {
	for i := 0; i < 30; i++ {
		reply := &mr.MRReply{}
		err := c.AssignTask(&mr.MRArgs{}, reply)
		err = c.FinishTask(&mr.MRArgs{reply.Task}, reply)
		if err == nil && reply.Task.Status == mr.FINISHED {
			t.Logf("Success: TestCallFinishTask")
		} else {
			t.Error("Fail: TestCallFinishTask")
		}
	}

}
func TestCallAssignTask(t *testing.T) {
	args := &mr.MRArgs{}
	reply := &mr.MRReply{}
	var err error
	for i := 0; i < mr.MAXWAITTIME; i++ {
		err = c.FinishTask(args, reply)
		if err != nil {
			log.Printf("warn: fail to finish the task, try again! %v\n", err)
		} else {
			break
		}
	}
	if err != nil {
		log.Printf("error: the worker had exceeded retry count of 'AssignTask'! %v\n", err)
	}
	t.Logf("Success: TestCallTask, mapid = %d\n", reply.Task.Id)
}
