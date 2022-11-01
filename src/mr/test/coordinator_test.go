package test

import (
	"6.824/mr"
	"fmt"
	"testing"
)

func TestMakeCoordinator(t *testing.T) {
	fmt.Println(c.MapTaskQueue.Len())
	fmt.Println(c.ReduceTaskQueue.Len())
	fmt.Println(len(c.AssignedMapTaskArr))
	fmt.Println(len(c.AssignedMapTaskArr))
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
		if reply.Task != nil {
			fmt.Printf("Assign task(%v) to worker%d\n", reply.Task, i)
		}
		if i%2 == 1 {
			args.Task, args.Task = reply.Task, reply.Task
			c.FinishTask(args, reply)
		}
	}
	t.Log("Success: TestAssignTask")
}

// 测试：正常情况结束任务
func TestFinishTask01(t *testing.T) {
	GenerateTestData()
	args := &mr.MRArgs{Task: &mr.MrTask{TaskType: 0, Id: 1, Status: 1}}
	c.AssignedMapTaskArr[1] = mapTask
	reply := &mr.MRReply{}
	c.FinishTask(args, reply)
	if reply.Task != nil && reply.Task.Status == mr.FINISHED {
		t.Log("Success: TestFinishTask")
	} else {
		t.Error("Fail: TestFinishTask")
	}
}

// 测试：目标任务在coordinator的assignedMap不存在
func TestFinishTask02(t *testing.T) {
	//generate test data...
	args := &mr.MRArgs{Task: &mr.MrTask{TaskType: 1, Id: 1, Status: 1}}
	reply := &mr.MRReply{}
	delete(c.AssignedReduceTaskArr, 1)
	c.FinishTask(args, reply)
	if reply.Task != nil && reply.Task.Status == mr.FINISHED {
		t.Error("Fail: TestFinishTask")
	} else {
		t.Log("Success: 测试：目标任务在coordinator的assignedMap不存在")
	}
}

func Test(t *testing.T) {

}
