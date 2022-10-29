package test

import (
	"6.824/mr"
	"log"
	"sync"
	"testing"
)

func TestMutiplyWorker(t *testing.T) {
	GenerateTestData()
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go WorkerFunc(&wg, i)
	}
	wg.Wait()
}
func WorkerFunc(wc *sync.WaitGroup, i int) {
	//log.Printf("worker%d", i)
	mr.Worker(mapf, reducef)
	wc.Done()
}
func TestWorker(t *testing.T) {
	GenerateTestData()
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go mr.Worker(mapf, reducef)
		wg.Done()
	}
	wg.Wait()
}

func TestCallFinishTask(t *testing.T) {
	GenerateTestData()
	for i := 0; i < 30; i++ {
		reply := &mr.MRReply{}
		err := mr.CallCoordinator(&mr.MRArgs{}, reply, "AssignTask")
		err = mr.CallCoordinator(&mr.MRArgs{reply.MapTask, reply.ReduceTask}, reply, "FinishTask")
		if err == nil && reply.MapTask.Status == mr.FINISHED {
			t.Logf("Success: TestCallFinishTask")
		} else {
			t.Error("Fail: TestCallFinishTask")
		}
	}

}
func TestCallAssignTask(t *testing.T) {
	GenerateTestData()
	args := &mr.MRArgs{}
	reply := &mr.MRReply{}
	var err error
	for i := 0; i < mr.MAXWAITTIME; i++ {
		err = mr.CallCoordinator(args, reply, "AssignTask")
		if err != nil {
			log.Printf("warn: fail to finish the task, try again! %v\n", err)
		} else {
			break
		}
	}
	if err != nil {
		log.Printf("error: the worker had exceeded retry count of 'AssignTask'! %v\n", err)
	}
	t.Logf("Success: TestCallTask, mapid = %d\n", reply.MapTask.Id)
}
