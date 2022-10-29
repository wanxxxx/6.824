package test

import (
	"6.824/mr"
	"fmt"
	"testing"
)

func TestMakeCoordinator(t *testing.T) {
	fmt.Println(c.MapTaskQueue.Len())
	fmt.Println(c.ReduceTaskQueue.Len())
	fmt.Println(len(c.AssignedMapTaskMap))
	fmt.Println(len(c.AssignedMapTaskMap))
	t.Log("Success: TestMakeCoordinator")
}

func TestAssignTask(t *testing.T) {
	// 测试所有情况，分配map/分配reduce/无任务可分配
	// case02
	for i := 0; i < 30; i++ {
		args := &mr.MRArgs{}
		reply := &mr.MRReply{}
		err := c.AssignTask(args, reply)
		if err != nil {
			fmt.Printf("Failed to assign task for worker%d, %v\n", i, err)
		}
		if reply.MapTask != nil {
			fmt.Printf("Assign maptask%d to worker%d\n", reply.MapTask.Id, i)
		}
		if reply.ReduceTask != nil {
			fmt.Printf("Assign reducetask%d to worker%d\n", reply.ReduceTask.Id, i)
		}
		if i%2 == 1 {
			args.MapTask, args.ReduceTask = reply.MapTask, reply.ReduceTask
			c.FinishTask(args, reply)
		}
	}
	t.Log("Success: TestAssignTask")
}

// 测试：正常情况结束任务
func TestFinishTask01(t *testing.T) {
	args := &mr.MRArgs{&mapTask, nil}
	c.AssignedMapTaskMap[mapTask.Id] = &mapTask
	reply := &mr.MRReply{}
	c.FinishTask(args, reply)
	if reply.MapTask.Status == mr.FINISHED {
		t.Log("Success: TestFinishTask")
	}
}

// 测试：目标任务在coordinator的assignedMap不存在
func TestFinishTask02(t *testing.T) {
	//generate test data...
	GenerateTestData()
	args := &mr.MRArgs{&mr.MapTask{1, "", 1}, nil}
	reply := &mr.MRReply{}
	delete(c.AssignedReduceTaskMap, 1)
	err := c.FinishTask(args, reply)
	if err == nil && reply.MapTask.Status == mr.FINISHED {
		t.Log("Success: TestFinishTask")
	} else {
		t.Error(err)
	}
}

func TestDone(t *testing.T) {
	GenerateTestData()
	mr.Worker(mapf, reducef)

	if c.Done() {
		t.Log("Success: TestDone")
	} else {
		t.Error("Fail: not all tasks are done")
	}

}
func Test(t *testing.T) {

}
