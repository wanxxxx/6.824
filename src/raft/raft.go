package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, LeaderTerm, isLeader)
//   start agreement on a new log entry
// rf.GetState() (LeaderTerm, isLeader)
//   ask a Raft for its current LeaderTerm, and whether it thinks it is rf
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

const (
	MIN_ElECTION_MS = 1000
	MAX_ElECTION_MS = 1500
	TIME_UNIT       = time.Millisecond
	BROADCAST_TIME  = 50 * TIME_UNIT // 一个服务器将RPC并行发给集群中所有服务器（不包括其自身）并收到响应的平均时间，由机器本身特性决定
	FOLLOWER        = 0
	LEADER          = 1
	CANDIDATE       = 2
	INVALID         = math.MinInt
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	muMap     map[string]*sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers(Updated on stable storage before responding to RPCs)
	currentTerm int
	log         []*LogEntry // log entries from client or rf
	voteFor     int

	// Volatile state on all servers:
	commitIndex      int64 //Index of the highest committed log
	lastAppliedIndex int64 //Index of the highest log entry applied to state machine

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int64 // Index of the next log entry to followers (initialized to rf last log Index+1)
	matchIndex []int64 // Index of highest log entry which is replicated on server

	voteCount     int64
	applyCh       chan ApplyMsg
	lastHeartbeat time.Time // update after receive heartbeat from rf, vote for a candidate
	state         int       // changed in the start and end of election and request or response rpc
	snapshot      []byte
	lastIndex     int64
}

// return currentTerm and whether this server
// believes it is the rf.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}

// get the guard node's index
// the index of log ∈ (initialIndex, lastIndex]
func (rf *Raft) getLastIncludeIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) getLastIndex() int {
	if len(rf.log) < 1 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getEntry(index int) *LogEntry {
	if index > rf.getLastIndex() || index < rf.getLastIncludeIndex() {
		return nil
	}
	return rf.log[index-rf.getLastIncludeIndex()]
}

func (rf *Raft) getSliceOfLog(s int, e int) []*LogEntry {
	start := rf.getLastIncludeIndex()
	if s == INVALID {
		s = rf.getLastIncludeIndex()
	}
	if e == INVALID {
		e = rf.getLastIndex()
	}
	if s < start || e > rf.getLastIndex() || s > e {
		//fmt.Println("Out of bounds")
		return nil
	}

	//defer rf.persist()
	return rf.log[s-start : e-start+1]
}

// call after follower receive rf's commitLogs or rf success to send log
func (rf *Raft) updateCommit(newCommit int) bool {
	oldCommit := rf.commitIndex
	// commitIndex <= lastLogIndex)
	newCommit = MinInt(newCommit, rf.getLastIndex())
	var ok bool
	if int64(newCommit) > oldCommit {
		ok = atomic.CompareAndSwapInt64(&rf.commitIndex, oldCommit, int64(newCommit))
		if !ok {
			//fmt.Println("Cas fail")
		}
	}
	if ok {
		fmt.Printf(rf.toString()+" commit (%v)\n", rf.getEntry(newCommit))
		go rf.applyEntries()
	}
	return ok
}

func (rf *Raft) updatePersistSate(currentTerm int, voteFor int, log []*LogEntry) bool {
	if currentTerm == INVALID && voteFor == INVALID && log == nil {
		return false
	}
	rf.raftLock("persist")
	defer rf.raftUnlock("persist")
	if currentTerm != INVALID {
		if currentTerm < rf.currentTerm {
			fmt.Println("Warn: term cannot go back")
			return false
		} else if currentTerm > rf.currentTerm {
			voteFor = -1
		} else if voteFor >= 0 && voteFor != rf.voteFor && rf.voteFor != -1 {
			fmt.Println("Warn: already vote for other in current term")
			return false
		}
		rf.currentTerm = currentTerm
	}
	if voteFor != INVALID {
		rf.voteFor = voteFor
	}
	if log != nil {
		rf.log = log
	}
	rf.persist()
	return true

}

func (rf *Raft) raftLock(lockName string) {
	_, ok := rf.muMap[lockName]
	if !ok {
		rf.muMap[lockName] = &sync.Mutex{}
	}
	rf.muMap[lockName].Lock()
}

func (rf *Raft) raftUnlock(lockName string) {
	rf.muMap[lockName].Unlock()
}

func (rf *Raft) isHeartBeat() bool {
	randTime := getRandTime(MIN_ElECTION_MS, MAX_ElECTION_MS)
	time.Sleep(randTime)
	nowTime := time.Now()
	if nowTime.Sub(rf.lastHeartbeat) > randTime && !rf.isLeader() {
		//fmt.Printf(rf.toString()+" timeout=%v, nowTime=%v\n", randTime, nowTime.Format("15:04:05.00"))
		return false
	}
	//fmt.Printf(rf.toString() + " received heartbeat...\n")
	return true
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	if rf.persister == nil {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	err = e.Encode(rf.voteFor)
	err = e.Encode(rf.log)
	if err != nil {
		log.Printf("persist error")
	}
	Command := w.Bytes()
	rf.persister.SaveRaftState(Command)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logEntries []*LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logEntries) != nil {
		//fmt.Printf("de\n")
		return
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = logEntries
	}
}

func (rf *Raft) readSnapshot(data []byte) (int, []interface{}) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var xlog []interface{}
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xlog) != nil {
		log.Fatalf("snapshot decode error")
	}
	return lastIncludedIndex, xlog
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// todo Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.

// In Lab 2D, the tester calls Snapshot() periodically.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.raftLock("log")
	defer rf.raftUnlock("log")
	if rf.getEntry(index) == nil {
		return
	}
	lastIncludeIndex, xlog := rf.readSnapshot(snapshot)
	if lastIncludeIndex != len(xlog)-1 {
		log.Fatalf(rf.toString() + " last included index doesn't match")
	}
	rf.snapshot = snapshot
	rf.updatePersistSate(INVALID, INVALID, rf.getSliceOfLog(index, INVALID))
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int //candidate's Term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //Index of candidate's last committed log entry($5.4)
	LastLogTerm  int //Term of candidate's last committed log entry(5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // current Term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	LeaderTerm        int         // rf's term
	LeaderId          int         // so follower can redirect clients
	PrevLogIndex      int         // Index of log entry immediately preceding new ones, is always equals with nextIndex-1
	PrevLogTerm       int         // LeaderTerm of prevLogIndex entry
	Entries           []*LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIndex int         // rf's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for rf to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // the first index of the conflicting entry
	//ConflictTerm  int   // the term of the conflicting entry
}

type InstallSnapshotArgs struct {
	LeaderTerm        int    // rf's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    //the snapshot replaces all entries up through  and including this index
	LastIncludedTerm  int    //term of LastIncludedIndex
	Offset            int    //byte Offset where chunk is positioned in the  snapshot file
	Data              []byte //raw bytes of the snapshot chunk, starting at  Offset
	Done              bool   //true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for rf to update itself
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) toString() string {
	var stateName string
	switch rf.state {
	case 0:
		stateName = "F"
	case 1:
		stateName = "L"
	case 2:
		stateName = "C"
	}
	return fmt.Sprintf(stateName+"(id=%d, term=%d)", rf.me, rf.currentTerm)
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the rf, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the rf
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// LeaderTerm. the third return value is true if this server believes it is
// the rf.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if !rf.isLeader() {
		return 0, rf.currentTerm, false
	}
	rf.raftLock("log")
	defer func() {
		rf.raftUnlock("log")
		rf.persist()
		fmt.Printf(rf.toString()+" add new log(%d: %v)\n", rf.getLastIndex(), command)
	}()
	entry := &LogEntry{rf.currentTerm, rf.getLastIndex() + 1, command}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = int64(entry.Index)
	return entry.Index, rf.currentTerm, rf.isLeader()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// todo Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// If RPC request or response contains LeaderTerm T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) afterRPC(argTerm int) bool {
	if argTerm > rf.currentTerm {
		if rf.isLeader() {
			fmt.Printf(rf.toString() + "turn to follower\n")
		}
		rf.state = FOLLOWER
		rf.updatePersistSate(argTerm, -1, nil)
		return false
	}
	return true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.raftLock("vote")
	defer func() {
		rf.raftUnlock("vote")

	}()
	rf.afterRPC(args.Term)
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	lastLog := rf.getEntry(rf.getLastIndex())
	if args.Term >= rf.currentTerm && rf.voteFor == -1 &&
		compareLog(lastLog.Term, args.LastLogTerm, lastLog.Index, args.LastLogIndex) <= 0 {
		rf.updatePersistSate(args.Term, args.CandidateId, nil)
		reply.VoteGranted = true

	}
	reply.Term = MaxInt(args.Term, rf.currentTerm)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = MaxInt(rf.currentTerm, args.LeaderTerm)
	// 1. Reply false if term < currentTerm
	if args.LeaderTerm < rf.currentTerm {
		//fmt.Printf(rf.toString()+" deny AppendEntries from server(id=%d, term=%d), because lower term\n", args.LeaderId, args.LeaderTerm)
		return
	}
	rf.lastHeartbeat = time.Now()

	rf.raftLock("log")
	defer func() {
		rf.raftUnlock("log")
		rf.afterRPC(args.LeaderTerm)
	}()
	preLog := rf.getEntry(args.PrevLogIndex)
	newLog := rf.log
	if preLog != nil && preLog.Term == args.PrevLogTerm {
		reply.Success = true
		if args.Entries != nil && len(args.Entries) > 0 {
			lastArgEntry := args.Entries[len(args.Entries)-1]
			lastRfEntry := rf.getEntry(lastArgEntry.Index)
			// 3. If an existing entry conflicts with a new one (same Index but different terms), delete the existing entry and all successor
			if lastRfEntry == nil || lastRfEntry.Term != lastArgEntry.Term {
				newLog = append(rf.getSliceOfLog(INVALID, preLog.Index), args.Entries...)
			}
			rf.updatePersistSate(INVALID, INVALID, newLog)
		}
		// If leaderCommit>commitIndex,set commitIndex=min(leaderCommit, lastLogIndex)
		if args.LeaderCommitIndex > int(rf.commitIndex) {
			rf.updateCommit(args.LeaderCommitIndex)
		}
	} else {
		// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		reply.Success = false
		// There isn't exist same pre log, so find the first conflict entry
		if args.PrevLogIndex < rf.getLastIncludeIndex() {
			reply.ConflictIndex = rf.getLastIncludeIndex() + 1
		} else {
			conflictIndex := MaxInt(rf.getLastIncludeIndex()+1, MinInt(rf.getLastIndex(), args.PrevLogIndex))
			// find the first index with same term of last conflict entry
			for ; conflictIndex > rf.getLastIncludeIndex()+1; conflictIndex-- {
				if rf.getEntry(conflictIndex-1).Term != rf.getEntry(conflictIndex).Term {
					break
				}
			}
			reply.ConflictIndex = conflictIndex
		}
		//if args.PrevLogIndex > rf.getLastIndex() {
		//	//fmt.Printf(rf.toString()+" refuse commitLogs from L(%d). Because preLog(t=%d, conflictIndex=%d) isn't exist. F-lastLog(t=%d, conflictIndex=%d)\n", args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.getEntry(rf.getLastIndex()).Term, rf.getLastIndex())
		//} else {
		//	//fmt.Printf(rf.toString()+" refuse commitLogs from L(%d). Because L-preLog-Term(%d) != F-Term(%d)\n", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.getEntry(args.PrevLogIndex).Term)
		//}
		if preLog != nil {
			newLog = rf.getSliceOfLog(INVALID, args.PrevLogIndex-1)
		}
		rf.updatePersistSate(INVALID, INVALID, newLog)
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Term = MaxInt(rf.currentTerm, args.LeaderTerm)
	//1. Reply immediately if term < current Term
	if args.LeaderTerm < rf.currentTerm {
		return
	}
	rf.afterRPC(args.LeaderTerm)
	//2. Create new snapshot file if first chunk(Offset is 0)
	if args.Offset == 0 {
		rf.snapshot = []byte{}
	}
	//3. Write Data into snapshot file at given Offset
	rf.snapshot = append(rf.snapshot[:args.Offset], args.Data...)
	//4. Reply and wait for more Data chunks if Done is false
	if !args.Done {
		return
	}
	//5. Save snapshot file,
	rf.raftLock("applyEntries")
	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
	rf.lastAppliedIndex = int64(args.LastIncludedIndex)
	rf.raftUnlock("applyEntries")

	rf.raftLock("log")
	defer rf.raftUnlock("log")
	// discard any existing or partial snapshot with a smaller index
	includeLog := rf.getEntry(args.LastIncludedIndex)
	newLog := rf.log
	if includeLog != nil && rf.getLastIncludeIndex() < includeLog.Index {
		fmt.Printf(rf.toString()+" discard log before last include log(%d to %d)\n", rf.getLastIncludeIndex(), includeLog.Index-1)
		newLog = rf.getSliceOfLog(includeLog.Index, INVALID)
	} else if includeLog == nil || includeLog.Term != args.LastIncludedTerm {
		//6. If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
		//7. Discard the entire log
		if includeLog != nil {
			fmt.Printf(rf.toString()+" discard commitLogs(%d to %d), because the last included entry has diff term", includeLog.Index, rf.getLastIndex())
		}
		// set lastIncludeTerm as guard node
		newLog = []*LogEntry{{args.LastIncludedTerm, args.LastIncludedIndex, nil}}
	}
	//8. Reset state machine using snapshot contents (and load snapshot's cluster configuration)
	rf.updatePersistSate(INVALID, INVALID, newLog)
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if !rf.isLeader() {
		//fmt.Println(rf.toString() + " isn't rf, so can't send AppendEntries RPC")
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//if !ok && args.Entries != nil {
	//	//fmt.Printf(rf.toString()+" fail to send commitLogs RPC to server(id=%d), args is %v\n", server, args)
	//} else if !ok && args.Entries == nil {
	//	//fmt.Printf(rf.toString()+" fail to send heartbeats RPC to server(id=%d), args is %v\n", server, args)
	//} else {
	//	//fmt.Printf(rf.toString()+" success to send AppendEntries RPC to server(id=%d), args is %v\n", server, args)
	//}
	if !ok || args.LeaderTerm < reply.Term {

	} else if reply.Success {
		// Success to replicate: nextIndex = last log+1, matchIndex=last log
		oldMatch := int(rf.matchIndex[server])
		if args.Entries != nil {
			lastLogIndex := args.Entries[len(args.Entries)-1].Index
			if atomic.CompareAndSwapInt64(&rf.nextIndex[server], int64(args.PrevLogIndex+1), int64(lastLogIndex+1)) &&
				oldMatch < lastLogIndex && atomic.CompareAndSwapInt64(&rf.matchIndex[server], int64(oldMatch), int64(lastLogIndex)) {
				rf.checkCommit()
				//fmt.Printf(rf.toString()+" success to send commitLogs(%d-%d) to server(%d)", args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index, server)
			}
		} else if args.PrevLogIndex > oldMatch {
			atomic.CompareAndSwapInt64(&rf.matchIndex[server], int64(oldMatch), int64(args.PrevLogIndex))
		}
	} else if !reply.Success {
		atomic.CompareAndSwapInt64(&rf.nextIndex[server], int64(args.PrevLogIndex+1), int64(reply.ConflictIndex))
		//fmt.Printf(rf.toString()+" preLog(%d,%d) non-exist, nextIndex[%d]->(%d)\n", args.PrevLogIndex, args.PrevLogTerm, server, reply.ConflictIndex)
	}
	rf.afterRPC(reply.Term)
	return ok
}

func (rf *Raft) getVoteFrom(server int) {
	args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastIndex(), rf.getEntry(rf.getLastIndex()).Term}
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok && reply.VoteGranted && rf.state == CANDIDATE {
		atomic.AddInt64(&rf.voteCount, 1)
		//fmt.Printf(rf.toString()+" get 1 vote from server(id=%d), votecount=%d\n", server, rf.voteCount)
	}
	rf.afterRPC(reply.Term)
}

// sendHeartBeat isn't diff with sendLog
//func (rf *Raft) sendHeartBeatTo(server int) {
//	go func() {
//		for !rf.killed() && rf.isLeader() {
//			preIndex := rf.nextIndex[server] - 1
//
//			args := &AppendEntriesArgs{rf.currentTerm, rf.me, preIndex, rf.getEntry(preIndex].Term, nil, rf.commitIndex}
//			reply := &AppendEntriesReply{}
//			//fmt.Printf(rf.toString()+" send heartbear to server(%d)...", server)
//			rf.sendAppendEntriesRPC(server, args, reply)
//			time.Sleep(BROADCAST_TIME)
//		}
//	}()
//}

// logIndex is the index of first log entry
//
func (rf *Raft) sendLeaderInfoTo(server int) {
	for !rf.killed() && rf.isLeader() {
		nextIndex := int(rf.nextIndex[server])
		preLog := rf.getEntry(nextIndex - 1)
		if rf.getLastIncludeIndex() < nextIndex {
			// If last log index ≥ nextIndex: send AppendEntries RPC with log entries starting at nextIndex
			// if last log index < nextIndex: send AppendEntries RPC with empty entries as heartbeat
			logs := rf.getSliceOfLog(nextIndex, INVALID)
			args := &AppendEntriesArgs{rf.currentTerm, rf.me, preLog.Index, preLog.Term, logs, int(rf.commitIndex)}
			reply := &AppendEntriesReply{}
			rf.sendAppendEntriesRPC(server, args, reply)
		} else if nextIndex <= rf.getLastIncludeIndex() {
			// leader must occasionally send snapshots to followers when the leader has already
			// discarded the next log entry that it needs to send to a follower.
			rf.sendInstallSnapshotTo(server)
		}
		time.Sleep(BROADCAST_TIME)
	}
}

//func (rf *Raft) sendHeartBeatTo(server int) {
//	for !rf.killed() && rf.isLeader() {
//		nextIndex := int(rf.nextIndex[server])
//		preLog := rf.getEntry(nextIndex - 1)
//		if preLog != nil {
//			args := &AppendEntriesArgs{rf.currentTerm, rf.me, preLog.Index, preLog.Term, nil, int(rf.commitIndex)}
//			reply := &AppendEntriesReply{}
//			//fmt.Printf(rf.toString()+"send heartbeat to %d\n", server)
//			rf.sendAppendEntriesRPC(server, args, reply)
//		}
//		time.Sleep(BROADCAST_TIME)
//	}
//}
// If it doesn't have the log entries required to bring a follower up to date,
// the rf send an InstallSnapshot RPC
func (rf *Raft) sendInstallSnapshotTo(server int) {
	if rf.snapshot != nil {
		args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.log[0].Index, rf.log[0].Term, 0, rf.snapshot, true}
		reply := &InstallSnapshotReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		if ok {
			// TODO cas
			rf.matchIndex[server] = int64(args.LastIncludedIndex)
			rf.nextIndex[server] = int64(args.LastIncludedIndex + 1)
		}
	}
}

func (rf *Raft) afterWin() {
	rf.state = LEADER
	for i := range rf.nextIndex {
		rf.nextIndex[i] = int64(rf.getLastIndex() + 1)
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = int64(rf.getLastIncludeIndex())
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			//go func(i int) {
			//	rf.sendHeartBeatTo(i)
			//}(i)
			go func(i int) {
				rf.sendLeaderInfoTo(i)
			}(i)
		}
	}
}

// if this peer hasn't received heartbeats recently.

func (rf *Raft) ticker() {
	// heartBeats < election out < election time out
	//go rf.sendLogOrHeartBeats()
	//go rf.checkHeartBeat()
	//go rf.replicateLogs()
	//go rf.checkCommit()
	//go rf.applyEntries()
	for !rf.killed() {
		if !rf.isLeader() {
			rf.checkHeartBeat()
		} else {
			time.Sleep(BROADCAST_TIME)
		}
	}
}

// For non-rf to check heartBeat from rf
func (rf *Raft) checkHeartBeat() {
	for rf.state == FOLLOWER {
		if !rf.isHeartBeat() {
			rf.election()
		}
	}
}

// For rf update commit after sendLogOrHeartBeatTo() and then matchIndex changed
func (rf *Raft) checkCommit() {
	if rf.isLeader() {
		// The next index which need to be committed is the rf.commitIndex(as j) + 1
		// if matchIndex[i] > j, it means the commitLogs which index between [j+1,matchIndex[i]] are replicated to server i
		count := 0
		oldCommitIndex := rf.commitIndex
		targetIndex := math.MaxInt
		for _, index := range rf.matchIndex {
			if index > oldCommitIndex {
				targetIndex = MinInt(targetIndex, int(index))
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.updateCommit(targetIndex)
		}
	}

}

// For all server apply new committed commitLogs after update of commitIndex
func (rf *Raft) applyEntries() {
	rf.raftLock("applyEntries")
	for rf.lastAppliedIndex+1 <= rf.commitIndex && rf.applyCh != nil {
		e := rf.getEntry(int(rf.lastAppliedIndex + 1))
		if e == nil {
			break
		}
		rf.applyCh <- ApplyMsg{Command: e.Command, CommandValid: true, CommandIndex: int(rf.lastAppliedIndex + 1)}
		fmt.Printf(rf.toString()+" apply (%v)\n", e)
		rf.lastAppliedIndex++
	}
	rf.raftUnlock("applyEntries")

}

// The ticker go routine starts a new election

func (rf *Raft) election() {
	// if server vote for other server with same term or with larger term
	if rf.isLeader() {
		return
	}
	rf.updatePersistSate(rf.currentTerm+1, rf.me, nil)
	rf.voteCount = 1
	rf.state = CANDIDATE
	//fmt.Printf(rf.toString() + " start the election...\n")
	// issues RequestVote RPCs in parallel to other servers
	//fmt.Printf("Server(id=%d, item=%d) satrt the election\n", rf.me, rf.currentTerm)

	for i := 0; i < len(rf.peers) && rf.state == CANDIDATE; i++ {
		if i != rf.me {
			go func(i int) {
				rf.getVoteFrom(i)
			}(i)
		}
	}
	// 等待3个事件之一发生，选举就可结束
	eventCh := make(chan int)
	// 1. Win majority
	go func() {
		for int(rf.voteCount) <= len(rf.peers)/2 {
			time.Sleep(BROADCAST_TIME)
		}
		if rf.state == CANDIDATE {
			eventCh <- 0
		}
	}()

	// 2. Another server win
	go func() {
		// 1. if there is already a rf, stop election
		// 2. if there is already a candidate with higher term, stop election
		for !rf.isHeartBeat() && rf.state == CANDIDATE {
		}
		eventCh <- 1
	}()
	// 3. Time out and no winner
	go func() {
		time.Sleep(getRandTime(MIN_ElECTION_MS, MAX_ElECTION_MS))
		if rf.state == CANDIDATE {
			eventCh <- 2
		}
	}()

	res := <-eventCh
	switch res {
	case 0:
		rf.afterWin()
		//fmt.Printf(rf.toString() + " win the election\n")
	case 1:
		//fmt.Println(rf.toString() + " lost the election")
	case 2:
		//fmt.Printf(rf.toString() + " start the next election...\n")
		rf.election()
	}

}

// Make
//  @Description: create a Raft server.
//  	Make() must return quickly,
// 		so it should start goroutines for any long-running work.
//  @param peers: the ports of all the Raft servers (including this one),
// 		all the servers' peers[] arrays have the same order.
//  @param me: this server's port is peers[me]
//  @param persister: save server's persistent state and
// 		also initially holds the most recent saved state, if any.
//  @param applyCh: a channel to send ApplyMsg messages.
//  @return *Raft
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := initRaft(peers, me, persister, applyCh)
	// todo Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// Create a background goroutine that will kick off rf election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	go rf.ticker()

	return rf
}

func initRaft(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		muMap:       make(map[string]*sync.Mutex),
		peers:       peers,
		me:          me,
		persister:   persister,
		dead:        0,
		currentTerm: 0,
		log:         []*LogEntry{{0, 0, "dummy node"}}, // log entries from client or rf
		voteFor:     -1,                                // means null
		//voteForTerm:      -1,
		lastHeartbeat:    time.Now(),
		commitIndex:      0,
		lastAppliedIndex: 0,
		nextIndex:        make([]int64, len(peers)),
		matchIndex:       make([]int64, len(peers)),
		state:            FOLLOWER,
		voteCount:        0,
		applyCh:          applyCh,
	}
	// the log with index = 0 is invalid
	return rf
}
