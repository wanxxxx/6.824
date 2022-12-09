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

type wantS struct {
	term    int
	voteFor int
	reply   *RequestVoteReply
}

func (w *wantS) equals(w2 *wantS) bool {
	if w.term == w2.term && w.voteFor == w2.voteFor && w.reply.VoteGranted == w2.reply.VoteGranted && w.reply.Term == w2.reply.Term {
		return true
	}
	return false
}
func Test_RequestVote(t *testing.T) {

	tests := []struct {
		name  string
		rf    *Raft
		args  *RequestVoteArgs
		reply *RequestVoteReply
		want  *wantS
	}{
		{
			name:  "Vote for higher term and not vote",
			args:  &RequestVoteArgs{10, 0, 0, 0},
			reply: &RequestVoteReply{},
			rf:    &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 0, voteFor: -1, log: []*LogEntry{{0, 0, nil}}},
			want:  &wantS{10, 0, &RequestVoteReply{10, true}},
		},
		{
			name:  "Vote for higher term and already voted before",
			args:  &RequestVoteArgs{10, 0, 0, 0},
			reply: &RequestVoteReply{},
			rf:    &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 0, voteFor: 10, log: []*LogEntry{{0, 0, nil}}},
			want:  &wantS{10, 0, &RequestVoteReply{10, true}},
		},
		{
			name:  "Vote for same term and not vote",
			args:  &RequestVoteArgs{1, 0, 0, 0},
			reply: &RequestVoteReply{},
			rf:    &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 1, voteFor: -1, log: []*LogEntry{{0, 0, nil}}},
			want:  &wantS{1, 0, &RequestVoteReply{1, true}},
		},
		{
			name:  "Vote for same term and already voted before",
			args:  &RequestVoteArgs{1, 0, 0, 0},
			reply: &RequestVoteReply{},
			rf:    &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 1, voteFor: 10, log: []*LogEntry{{0, 0, nil}}},
			want:  &wantS{1, 10, &RequestVoteReply{1, false}},
		},
		{
			name:  "Expire last log",
			args:  &RequestVoteArgs{1, 0, 0, 0},
			reply: &RequestVoteReply{},
			rf:    &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 1, voteFor: 10, log: []*LogEntry{{1, 1, nil}}},
			want:  &wantS{1, 10, &RequestVoteReply{1, false}},
		},
		{
			name:  "lower term",
			args:  &RequestVoteArgs{1, 0, 1, 1},
			reply: &RequestVoteReply{},
			rf:    &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 10, voteFor: 10, log: []*LogEntry{{0, 0, nil}}},
			want:  &wantS{10, 10, &RequestVoteReply{10, false}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rf.RequestVote(tt.args, tt.reply)
			got := wantS{tt.rf.currentTerm, tt.rf.voteFor, tt.reply}
			if !got.equals(tt.want) {
				t.Errorf("got:%v, want:%v\n", got, tt.want)
			}
		})
	}
}

//func TestSendHeartBeat(t *testing.T) {
//	servers := 3
//	cfg := make_config(t, servers, false, false)
//	defer cfg.cleanup()
//	time.Sleep(time.Second * 10)
//	cfg.rafts[0].sendHeartBeatTo(1)
//	cfg.rafts[1].sendHeartBeatTo(2)
//	cfg.rafts[2].sendHeartBeatTo(0)
//}

func Test_RequestVote_Response_TIME_NonLock(t *testing.T) {
	times := 1

	s := time.Now()
	for i := 0; i < times; i++ {
		args := &RequestVoteArgs{1, 1, -1, -1}
		reply := &RequestVoteReply{}
		rf := initRaft(nil, 2, nil, nil)
		rf.RequestVote(args, reply)
	}
	elapsed := time.Now().Sub(s)
	fmt.Printf("Average RequestVote without lock runtime is %v us\n", float64(elapsed.Microseconds())/float64(times))

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
				rf:    &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 1, log: []*LogEntry{{0, 0, nil}}},
				args:  &AppendEntriesArgs{LeaderTerm: 1, LeaderId: 100},
				reply: &AppendEntriesReply{},
			},
			want: &AppendEntriesReply{Term: 1, Success: true},
		},
		{
			name: "Receive heartBeat from leader in previous terms",
			args: args{
				rf:    &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 2},
				args:  &AppendEntriesArgs{LeaderTerm: 1, LeaderId: 100},
				reply: &AppendEntriesReply{},
			},
			want: &AppendEntriesReply{Term: 2, Success: false},
		},
		{
			name: "PreEntry non-exist01",
			args: args{
				rf: &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 2,
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
				rf: &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 2,
					log: []*LogEntry{{0, 0, nil}, {1, 1, nil}, {2, 2, nil}}},
				args: &AppendEntriesArgs{LeaderTerm: 2, LeaderId: 100, PrevLogIndex: 4, PrevLogTerm: 3,
					Entries: []*LogEntry{{4, 5, nil}}},
				reply: &AppendEntriesReply{},
			},
			want: &AppendEntriesReply{Term: 2, ConflictIndex: 2},
		},
		{
			name: "Follower no commitLogs",
			args: args{
				rf: &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 2,
					log: []*LogEntry{{0, 0, nil}}},
				args: &AppendEntriesArgs{LeaderTerm: 2, LeaderId: 100, PrevLogIndex: 4, PrevLogTerm: 3,
					Entries: []*LogEntry{{4, 5, nil}}},
				reply: &AppendEntriesReply{},
			},
			want: &AppendEntriesReply{Term: 2, ConflictIndex: 1},
		},
		{
			name: "Follower no commitLogs",
			args: args{
				rf: &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 2,
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
	tests := []struct {
		name       string
		rfLog      []*LogEntry
		argEntries []*LogEntry
		want       []*LogEntry
	}{
		// TODO: Add test cases.
		{
			name:       "Test1 Fail, i <len, j < len",
			rfLog:      []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}},
			argEntries: []*LogEntry{{1, 1, nil}, {2, 2, nil}},
			want:       []*LogEntry{{0, 0, nil}, {1, 1, nil}, {2, 2, nil}},
		},
		{
			name:       "Test2 Fail, i <len, j < len",
			rfLog:      []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}},
			argEntries: []*LogEntry{{1, 1, nil}, {100, 2, nil}, {100, 3, nil}, {100, 4, nil}},
			want:       []*LogEntry{{0, 0, nil}, {1, 1, nil}, {100, 2, nil}, {100, 3, nil}, {100, 4, nil}},
		},
		{
			name:       "Test3 Fail, i == len, j < len",
			rfLog:      []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}},
			argEntries: []*LogEntry{{1, 1, nil}, {1, 2, nil}},
			want:       []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}},
		},
		{
			name:       "Test4 Fail, i < len, j == len",
			rfLog:      []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}},
			argEntries: []*LogEntry{{1, 1, nil}, {1, 2, nil}, {1, 3, nil}, {1, 4, nil}, {1, 5, nil}},
			want:       []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}, {1, 4, nil}, {1, 5, nil}},
		},
		{
			name:       "Test5 success, i == len, j == len",
			rfLog:      []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}},
			argEntries: []*LogEntry{{1, 1, nil}, {1, 2, nil}, {1, 3, nil}},
			want:       []*LogEntry{{0, 0, nil}, {1, 1, nil}, {1, 2, nil}, {1, 3, nil}},
		},
	}
	rf := &Raft{me: 0, muMap: map[string]*sync.Mutex{}, currentTerm: 1, log: []*LogEntry{{0, 0, nil}}}
	args := &AppendEntriesArgs{LeaderTerm: 1, LeaderId: 100}
	reply := &AppendEntriesReply{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf.log = tt.rfLog
			args.Entries = tt.argEntries
			rf.AppendEntries(args, reply)
			if got := rf.log; !compareLogs(got, tt.want) {
				t.Errorf("got = %v, want %v\n", got, tt.want)
			}
		})
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
			rf: &Raft{me: 0, muMap: map[string]*sync.Mutex{}, commitIndex: 2, state: LEADER, peers: make([]*labrpc.ClientEnd, 5), log: []*LogEntry{{1, 2, nil}, {1, 3, 101}, {1, 4, 102}},
				matchIndex: []int64{10, 3, 3, 1, 0}},
			want: 3,
		},
		{
			name: "2",
			rf: &Raft{me: 0, muMap: map[string]*sync.Mutex{}, commitIndex: 2, state: LEADER, peers: make([]*labrpc.ClientEnd, 5), log: []*LogEntry{{1, 2, nil}, {1, 3, 101}, {1, 4, 102}},
				matchIndex: []int64{10, 6, 4, 1, 0}},
			want: 4,
		},
		{
			name: "3",
			rf: &Raft{me: 0, muMap: map[string]*sync.Mutex{}, commitIndex: 2, state: LEADER, peers: make([]*labrpc.ClientEnd, 5), log: []*LogEntry{{1, 2, nil}, {1, 3, 101}, {1, 4, 102}},
				matchIndex: []int64{10, 6, 2, 1, 0}},
			want: 2,
		},
		{
			name: "4",
			rf: &Raft{me: 0, muMap: map[string]*sync.Mutex{}, commitIndex: 2, state: LEADER, peers: make([]*labrpc.ClientEnd, 5), log: []*LogEntry{{1, 2, nil}, {1, 3, 101}, {1, 4, 102}},
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
			if !compareLogs(tt.rf.log, log) {
				t.Errorf("log")
			}
		})
	}
}

func TestRaft_updatePersistSate(t *testing.T) {
	type fields struct {
		muMap            map[string]*sync.Mutex
		peers            []*labrpc.ClientEnd
		persister        *Persister
		me               int
		dead             int32
		currentTerm      int
		log              []*LogEntry
		voteFor          int
		commitIndex      int64
		lastAppliedIndex int64
		nextIndex        []int64
		matchIndex       []int64
		voteCount        int64
		applyCh          chan ApplyMsg
		lastHeartbeat    time.Time
		state            int
		snapshot         []byte
		lastIndex        int64
	}
	type args struct {
		currentTerm int
		voteFor     int
		log         []*LogEntry
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := &Raft{
				muMap:            tt.fields.muMap,
				peers:            tt.fields.peers,
				persister:        tt.fields.persister,
				me:               tt.fields.me,
				dead:             tt.fields.dead,
				currentTerm:      tt.fields.currentTerm,
				log:              tt.fields.log,
				voteFor:          tt.fields.voteFor,
				commitIndex:      tt.fields.commitIndex,
				lastAppliedIndex: tt.fields.lastAppliedIndex,
				nextIndex:        tt.fields.nextIndex,
				matchIndex:       tt.fields.matchIndex,
				voteCount:        tt.fields.voteCount,
				applyCh:          tt.fields.applyCh,
				lastHeartbeat:    tt.fields.lastHeartbeat,
				state:            tt.fields.state,
				snapshot:         tt.fields.snapshot,
				lastIndex:        tt.fields.lastIndex,
			}
			if got := rf.updatePersistSate(tt.args.currentTerm, tt.args.voteFor, tt.args.log); got != tt.want {
				t.Errorf("updatePersistSate() = %v, want %v", got, tt.want)
			}
		})
	}
}
