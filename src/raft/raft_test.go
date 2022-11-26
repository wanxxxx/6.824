package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRequestVoteTwice(t *testing.T) {

	rafts := make([]*Raft, 3)
	for i := 0; i < 3; i++ {
		rafts[i] = initRaft(make([]*labrpc.ClientEnd, 3), i, nil, nil)
	}

	for i := 0; i < 2; i++ {
		args := &RequestVoteArgs{rafts[i].currentTerm, rafts[i].me, -1, -1}
		reply := &RequestVoteReply{}
		rafts[2].RequestVote(args, reply)
	}
}

func TestSendHeartBeat(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()
	time.Sleep(time.Second * 10)
	cfg.rafts[0].sendHeartBeatTo(1)
	cfg.rafts[1].sendHeartBeatTo(2)
	cfg.rafts[2].sendHeartBeatTo(0)

}

func (rf *Raft) RequestVoteWG(wg *sync.WaitGroup) {
	go func() {
		args := &RequestVoteArgs{100, 100, -1, -1}
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)
		wg.Done()
	}()

}
func (rf *Raft) sendAppendEntriesWG(server int, wg *sync.WaitGroup) {
	go func() {
		args := &AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, nil, rf.commitIndex}
		reply := &AppendEntriesReply{}
		rf.sendAppendEntriesRPC(server, args, reply)
		//if !ok {
		//	log.Printf("fail")
		//} else {
		//	log.Printf("success")
		//}
		wg.Done()
	}()

}

func Test_RequestVote_Response_TIME_NonLock(t *testing.T) {
	times := 100

	s := time.Now()
	for i := 0; i < times; i++ {
		args := &RequestVoteArgs{1, 1, -1, -1}
		reply := &RequestVoteReply{}
		rf := initRaft(nil, 2, nil, nil)
		rf.RequestVote(args, reply)
	}
	elapsed := time.Now().Sub(s)
	t.Logf("Average RequestVote without lock runtime is %v us\n", float64(elapsed.Microseconds())/float64(times))

}

func (a *AppendEntriesReply) equals(b *AppendEntriesReply) bool {
	if a.Term == b.Term && a.Success == b.Success && a.ConflictIndex == b.ConflictIndex {
		return true
	}
	return false
}
func TestRaft_AppendEntries(t *testing.T) {
	type args struct {
		rf    *Raft
		args  *AppendEntriesArgs
		reply *AppendEntriesReply
	}
	tests := []struct {
		name string
		args args
		want *AppendEntriesReply
	}{
		// TODO: Add test cases.
		{
			name: "Normal heartBeat",
			args: args{
				rf:    &Raft{me: 0, currentTerm: 1, log: []*LogEntry{{0, 0, nil}}},
				args:  &AppendEntriesArgs{LeaderTerm: 1, LeaderId: 100},
				reply: &AppendEntriesReply{},
			},
			want: &AppendEntriesReply{Term: 1, Success: true},
		},
		{
			name: "Receive heartBeat from leader in previous terms",
			args: args{
				rf:    &Raft{me: 0, currentTerm: 2},
				args:  &AppendEntriesArgs{LeaderTerm: 1, LeaderId: 100},
				reply: &AppendEntriesReply{},
			},
			want: &AppendEntriesReply{Term: 2, Success: false},
		},
		{
			name: "PreEntry non-exist01",
			args: args{
				rf: &Raft{me: 0, currentTerm: 2,
					log: []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}}},
				args: &AppendEntriesArgs{LeaderTerm: 2, LeaderId: 100, PrevLogIndex: 4, PrevLogTerm: 3,
					Entries: []*LogEntry{{4, 5, nil}}},
				reply: &AppendEntriesReply{},
			},
			want: &AppendEntriesReply{Term: 2, ConflictIndex: 1},
		},
		{
			name: "PreEntry non-exist02",
			args: args{
				rf: &Raft{me: 0, currentTerm: 2,
					log: []*LogEntry{{0, 0, nil}, {1, 1, nil}, {2, 2, nil}}},
				args: &AppendEntriesArgs{LeaderTerm: 2, LeaderId: 100, PrevLogIndex: 4, PrevLogTerm: 3,
					Entries: []*LogEntry{{4, 5, nil}}},
				reply: &AppendEntriesReply{},
			},
			want: &AppendEntriesReply{Term: 2, ConflictIndex: 2},
		},
		{
			name: "Follower no logs",
			args: args{
				rf: &Raft{me: 0, currentTerm: 2,
					log: []*LogEntry{{0, 0, nil}}},
				args: &AppendEntriesArgs{LeaderTerm: 2, LeaderId: 100, PrevLogIndex: 4, PrevLogTerm: 3,
					Entries: []*LogEntry{{4, 5, nil}}},
				reply: &AppendEntriesReply{},
			},
			want: &AppendEntriesReply{Term: 2, ConflictIndex: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.args.rf.AppendEntries(tt.args.args, tt.args.reply)
			if got := tt.args.reply; !got.equals(tt.want) {
				t.Errorf("got = %v, want %v\n", got, tt.want)
			}
		})
	}

}

func TestRaft_AppendEntries1(t *testing.T) {
	args := &AppendEntriesArgs{
		LeaderTerm:   1,
		LeaderId:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0}
	reply := &AppendEntriesReply{}
	rf := initRaft(nil, 2, nil, nil)

	rf.log = []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}}
	args.Entries = []*LogEntry{{1, 1, nil}, {2, 2, nil}}
	fmt.Printf("leni=%d, lenj=%d\n", len(args.Entries), len(rf.log))
	rf.AppendEntries(args, reply)
	if rf.log[2].Term != args.Entries[1].Term || len(rf.log) != 3 {
		t.Error("Test1 Fail, i <len, j < len")
	} else {
		t.Log("Test1 success, i <len, j < len")
	}

	rf.log = []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}}
	args.Entries = []*LogEntry{{1, 1, nil}, {100, 2, nil}, {100, 3, nil}, {100, 4, nil}}
	fmt.Printf("leni=%d, lenj=%d\n", len(args.Entries), len(rf.log))
	rf.AppendEntries(args, reply)
	if rf.log[2].Term != args.Entries[1].Term || len(rf.log) != 5 || rf.log[4].Term != 100 {
		t.Error("Test2 Fail, i <len, j < len")
	} else {
		t.Log("Test2 success, i <len, j < len")
	}

	rf.log = []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}}
	args.Entries = []*LogEntry{{1, 1, nil}, {1, 2, nil}}
	fmt.Printf("leni=%d, lenj=%d\n", len(args.Entries), len(rf.log))
	rf.AppendEntries(args, reply)
	if rf.log[2].Term != args.Entries[1].Term || len(rf.log) != 4 {
		t.Error("Test3 Fail, i == len, j < len")
	} else {
		t.Log("Test3 success, i == len, j < len")
	}

	rf.log = []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}}
	args.Entries = []*LogEntry{{1, 1, nil}, {1, 2, nil}, {1, 3, nil}, {1, 4, nil}, {1, 5, nil}}
	fmt.Printf("leni=%d, lenj=%d\n", len(args.Entries), len(rf.log))
	rf.AppendEntries(args, reply)
	if rf.log[5].Term != args.Entries[4].Term || len(rf.log) != 6 {
		t.Error("Test4 Fail, i < len, j == len")
	} else {
		t.Log("Test4 success, i < len, j == len")
	}

	rf.log = []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}}
	args.Entries = []*LogEntry{{1, 1, nil}, {1, 2, nil}, {1, 3, nil}}
	fmt.Printf("leni=%d, lenj=%d\n", len(args.Entries), len(rf.log))
	rf.AppendEntries(args, reply)
	if rf.log[3].Term != args.Entries[2].Term || len(rf.log) != 4 {
		t.Error("Test5 Fail, i == len, j == len")
	} else {
		t.Log("Test5 success, i == len, j == len")
	}

}

func TestRaft_RequestVote(t *testing.T) {

	rf := initRaft(nil, 0, nil, nil)
	rf.currentTerm = 1
	a := func(i int) {
		args := &RequestVoteArgs{1, i, 1, 1}
		reply := &RequestVoteReply{}
		go rf.RequestVote(args, reply)
	}
	for i := 1; i <= 10; i++ {
		a(i)
	}
	time.Sleep(time.Second)

}

func TestRaft_checkCommit(t *testing.T) {
	tests := []struct {
		name string
		rf   *Raft
		want int64
	}{
		{
			name: "1",
			rf: &Raft{me: 0, commitIndex: 2, state: LEADER, peers: make([]*labrpc.ClientEnd, 5), log: make([]*LogEntry, 5),
				matchIndex: []int64{10, 3, 3, 1, 0}},
			want: 3,
		},
		{
			name: "2",
			rf: &Raft{me: 0, commitIndex: 2, state: LEADER, peers: make([]*labrpc.ClientEnd, 5), log: make([]*LogEntry, 5),
				matchIndex: []int64{10, 6, 4, 1, 0}},
			want: 4,
		},
		{
			name: "3",
			rf: &Raft{me: 0, commitIndex: 2, state: LEADER, peers: make([]*labrpc.ClientEnd, 5), log: make([]*LogEntry, 5),
				matchIndex: []int64{10, 6, 2, 1, 0}},
			want: 2,
		},
		{
			name: "4",
			rf: &Raft{me: 0, commitIndex: 2, state: LEADER, peers: make([]*labrpc.ClientEnd, 5), log: make([]*LogEntry, 5),
				matchIndex: []int64{10, 2, 2, 1, 0}},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rf.checkCommit()
			if got := tt.rf.commitIndex; got != tt.want {
				t.Errorf("checkCommit() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestRaft_persist01(t *testing.T) {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(1)
	e.Encode(2)
	e.Encode([]*LogEntry{{0, 0, nil}, {1, 1, 101}, {1, 2, 102}})
	Command := w.Bytes()

	r := bytes.NewBuffer(Command)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int64
	var log []*LogEntry
	d.Decode(&currentTerm)
	d.Decode(&voteFor)
	d.Decode(&log)
}
func TestRaft_persist02(t *testing.T) {

	rf := &Raft{persister: MakePersister(),
		currentTerm: 1, voteFor: 1,
		log: []*LogEntry{{0, 0, nil}, {1, 1, 101}, {1, 2, 102}}}
	rf.persist()
	rf.readPersist(rf.persister.ReadRaftState())
	tests := []struct {
		name string
		rf   *Raft
	}{
		{
			name: "1",
			rf: &Raft{persister: MakePersister(),
				currentTerm: 1, voteFor: 1,
				log: []*LogEntry{{0, 0, nil}, {1, 1, 101}, {1, 2, 102}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			currentTerm := tt.rf.currentTerm
			voteFor := tt.rf.voteFor
			log := tt.rf.log
			tt.rf.persist()
			tt.rf.readPersist(tt.rf.persister.ReadRaftState())
			if tt.rf.currentTerm != currentTerm {
				t.Errorf("currentTerm")
			}
			if tt.rf.voteFor != voteFor {

				t.Errorf("voteFor")
			}
			if compareLogs(tt.rf.log, log) {
				t.Errorf("log")
			}
		})
	}
}