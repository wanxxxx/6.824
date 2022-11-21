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
//   ask a Raft for its current LeaderTerm, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"log"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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
	TIME_UNIT       = time.Millisecond // todo
	BROADCAST_TIME  = 10               // 一个服务器将RPC并行发给集群中所有服务器（不包括其自身）并收到响应的平均时间，由机器本身特性决定
	FOLLOWER        = 0
	LEADER          = 1
	CANDIDATE       = 2
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
	// todo
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// todo Your Command here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers(Updated on stable storage before responding to RPCs)
	currentTerm int
	log         []*LogEntry // log entries from client or leader
	logMu       sync.Mutex  //  1. Start 2. Append RPC

	voteFor int64 // lock by
	voteMu  sync.Mutex

	lastHeartbeat time.Time // update after receive heartbeat from leader, vote for a candidate
	state         int       // changed in the start and end of election and request or response rpc

	// Volatile state on all servers:
	commitIndex      int64 //Index of the highest committed log
	lastAppliedIndex int   //Index of the highest log entry applied to state machine
	// 1. commitIndex changed in checkCommit or AppendEntries
	// 2. vote for leader
	commitMu sync.Mutex

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int64 // Index of the next log entry to followers (initialized to leader last log Index+1)
	matchIndex []int64 // Index of highest log entry which is replicated on server
	indexMu    sync.Mutex

	// candidate
	voteCount int32
	applyCh   chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// todo Your code here (2A).

	return rf.currentTerm, rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}

func (rf *Raft) isHeartBeat() bool {
	randTime := getRandTime(MIN_ElECTION_MS, MAX_ElECTION_MS)
	time.Sleep(randTime)
	if time.Now().Sub(rf.lastHeartbeat) > randTime {
		return false
	}
	log.Printf(rf.toString() + " received heartbeat...")
	return true
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// todo Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// Command := w.Bytes()
	// rf.persister.SaveRaftState(Command)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// todo Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(Command)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// todo Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// todo Your Command here (2A, 2B).
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
	// todo Your Command here (2A).
	Term        int  // current Term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	LeaderTerm        int         //leader's LeaderTerm
	LeaderId          int         //so follower can redirect clients
	PrevLogIndex      int         // Index of log entry immediately preceding new ones
	PrevLogTerm       int         // LeaderTerm of prevLogIndex entry
	Entries           []*LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIndex int         //leader's commitIndex

}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
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
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// LeaderTerm. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// todo Your code here (2B).
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	if !rf.isLeader() {
		return 0, rf.currentTerm, false
	}
	// make sure the visibility of rf.state
	entry := &LogEntry{rf.currentTerm, len(rf.log), command}
	rf.log = append(rf.log, entry)
	go rf.replicateLog()
	log.Printf(rf.toString()+" append new log(%d)", len(rf.log)-1)
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf(rf.toString()+" receive RequestVote RPC from server(id=%d, term=%d)", args.CandidateId, args.Term)
	// todo Your code here (2A, 2B).
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	//rf.voteMu.Lock()
	if (rf.isLeader() && args.Term == rf.currentTerm) ||
		rf.currentTerm > args.Term {
		log.Printf(rf.toString()+" deny vote for server(id=%d, term=%d), because there exist higher term leader", args.CandidateId, args.Term)
		return
	}
	if args.Term > rf.currentTerm ||
		(args.Term == rf.currentTerm && (rf.voteFor == -1)) {
		rf.lastHeartbeat = time.Now()
	}
	reply.Term = MaxInt(rf.currentTerm, args.Term)
	rf.afterRPC(args.Term)
	if rf.voteFor != -1 {
		log.Printf(rf.toString()+" deny vote for server(id=%d, term=%d), because voted for (id=%d, term=%d)", args.CandidateId, args.Term, rf.voteFor, rf.currentTerm)
		return
	}
	lastLog := rf.log[len(rf.log)-1]
	if compareLog(lastLog.Term, lastLog.Index, args.LastLogTerm, args.LastLogIndex) > 0 {
		log.Printf(rf.toString()+" deny vote for server(id=%, term=%d), because its last log is up-to-date than server", args.CandidateId, args.Term)
		return
	}

	//if rf.voteFor == -1 && compareLog(lastLog.Term, lastLog.Index, args.LastLogTerm, args.LastLogIndex) <= 0 {
	// 如果收到投票请求，说明leader宕机了，rf大概率在选举超时前收不到新leader的心跳
	// 投票后，term会增加，很有可能rf会启动新一轮的选举
	// 所以这里将投票成功也看做心跳，减少同一term的选举竞争
	ok := atomic.CompareAndSwapInt64(&rf.voteFor, -1, int64(args.CandidateId))
	if ok {
		reply.VoteGranted, reply.Term = true, args.Term
		log.Printf(rf.toString()+" vote for (id=%d, term=%d)", args.CandidateId, args.Term)
	} else {
		log.Printf("votefor changed")
	}
	//}

	//rf.voteMu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if (rf.isLeader() && args.LeaderTerm == rf.currentTerm) ||
		rf.currentTerm > args.LeaderTerm {
		log.Printf(rf.toString()+" deny append entries from server(id=%d, term=%d), because there exist higher term leader", args.LeaderId, args.LeaderTerm)
		return
	}
	rf.logMu.Lock()
	defer func() {
		reply.Term = MaxInt(rf.currentTerm, args.LeaderTerm)
		rf.afterRPC(args.LeaderTerm)
		rf.logMu.Unlock()
	}()
	// 1. Reply false if term < currentTerm
	if args.LeaderTerm >= rf.currentTerm {
		rf.lastHeartbeat = time.Now()
		reply.Term = rf.currentTerm
		// heartbeat with empty log
		if args.Entries == nil || len(args.Entries) == 0 {
			reply.Success = true
			//log.Printf(rf.toString()+" receive heartbeat from leader(id=%d, term=%d)\n", args.LeaderId, args.LeaderTerm)
		} else if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
			// 3. If an existing entry conflicts with a new one (same Index but different terms), delete the existing entry and all successor

			i := 0
			j := args.PrevLogIndex + 1
			for i < len(args.Entries) && j < len(rf.log) {
				if args.Entries[i].compare(rf.log[j]) == 0 {
					i++
					j++
				} else {
					log.Printf(rf.toString()+" abondon logs from %d, (leader=%v, follower=%v)", j, args.Entries[i], rf.log[j])
					rf.log = rf.log[:j]
					break
				}
			}
			//fmt.Printf("i=%d, j=%d\n", i, j)
			// Append any new entries not already in the log
			if i < len(args.Entries) {
				rf.log = append(rf.log, args.Entries[i:]...)
				log.Printf(rf.toString()+" append logs(%d-%d) from leader%d", args.Entries[i].Index, args.Entries[len(args.Entries)-1].Index, args.LeaderId)
			}
			reply.Success = true

		} else {
			log.Printf(rf.toString()+" refuse logs from L(id=%d, term=%d), because preLog(%d) isn't consistent,  logCount %d->%d\n", args.LeaderId, args.LeaderTerm, args.PrevLogIndex, len(rf.log)-1, MinInt(args.PrevLogIndex, len(rf.log))-1)
			rf.log = rf.log[:MinInt(args.PrevLogIndex, len(rf.log))]
			reply.Success = false
		}
	}
	// If leaderCommit>commitIndex,set commitIndex=min(leaderCommit, lastLogIndex)
	//rf.commitMu.Lock()
	//defer rf.commitMu.Unlock()
	oldIndex := rf.commitIndex
	if int64(args.LeaderCommitIndex) > oldIndex {
		targetIndex := int64(MinInt(args.LeaderCommitIndex, len(rf.log)-1))
		ok := atomic.CompareAndSwapInt64(&rf.commitIndex, oldIndex, targetIndex)
		//rf.commitIndex = MinInt(args.LeaderCommitIndex, len(rf.log)-1)
		if ok {
			go rf.applyEntry()
			log.Printf(rf.toString()+" commitIndex %d->%d, because leaderCommitIndex=%d", oldIndex, targetIndex, args.LeaderCommitIndex)
		}
	}
}

func (rf *Raft) sendRequestVoteRPC(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//log.Printf(" (id=%d, term=%d) send request vote from server(id=%d)...", args.CandidateId, args.Term, server)
	if rf.state != CANDIDATE {
		log.Println(rf.toString() + " isn't candidate, so can't request votes")
		return false
	}
	defer rf.afterRPC(reply.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		//log.Printf(" (id=%d, term=%d) requestVote RPC failed from server(id=%d)", args.CandidateId, args.Term, server)
	} else {
		//log.Printf(" (id=%d, term=%d) requestVote RPC response from server(id=%d)", args.CandidateId, args.Term, server)

	}
	return ok
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	if !rf.isLeader() {
		//log.Println(rf.toString() + " isn't leader, so can't send AppendEntries RPC")
		return false
	}
	defer rf.afterRPC(reply.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//if !ok {
	//	//log.Printf(rf.toString()+" fail to send AppendEntries RPC to server(id=%d), args is %v\n", server, args)
	//} else {
	//	log.Printf(rf.toString()+" success to send AppendEntries RPC to server(id=%d), args is %v\n", server, args)
	//}
	return ok
}

func (rf *Raft) getVoteFrom(server int) {
	if rf.state != CANDIDATE || server == rf.me {
		return
	}
	go func() {
		args := &RequestVoteArgs{rf.currentTerm, rf.me, int(rf.commitIndex), rf.log[rf.commitIndex].Term}
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVoteRPC(server, args, reply)
		if ok && reply.VoteGranted && rf.state == CANDIDATE {
			atomic.AddInt32(&rf.voteCount, 1)
			log.Printf(rf.toString()+" get 1 vote from server(id=%d), votecount=%d\n", server, rf.voteCount)
		}
		//else if ok && !reply.VoteGranted {
		//	log.Printf(rf.toString()+" didn't get vote from server(id=%d)\n", server)
		//}
	}()
}

func (rf *Raft) sendHeartBeatTo(server int) {
	for !rf.killed() && rf.isLeader() {
		args := &AppendEntriesArgs{
			LeaderTerm:        rf.currentTerm,
			LeaderId:          rf.me,
			Entries:           nil,
			LeaderCommitIndex: int(rf.commitIndex)}
		reply := &AppendEntriesReply{}
		//log.Printf(rf.toString()+" send heartbear to server(%d)...", server)
		rf.sendAppendEntriesRPC(server, args, reply)
		time.Sleep(BROADCAST_TIME)
	}

}

// logIndex is the index of first log entry
func (rf *Raft) sendLogTo(logIndex, server int) {
	if rf.me == server || !rf.isLeader() || logIndex >= len(rf.log) || logIndex < 1 {
		return
	}
	go func() {
		args := &AppendEntriesArgs{rf.currentTerm, rf.me, logIndex - 1, rf.log[logIndex-1].Term, rf.log[logIndex:], int(rf.commitIndex)}
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntriesRPC(server, args, reply)
		if ok {
			//rf.indexMu.Lock()
			//defer rf.indexMu.Unlock()
			oldNext := rf.nextIndex[server]
			oldMatch := rf.matchIndex[server]
			if !reply.Success {
				casOk := atomic.CompareAndSwapInt64(&rf.nextIndex[server], oldNext, int64(logIndex-1))
				if casOk {
					log.Printf(rf.toString()+" preLog(%d) is inconsistent with (id=%d), decrements nextIndex to %d", args.PrevLogIndex, server, logIndex-1)
				}
			} else {
				//	rf.me, rf.currentTerm, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index, server, args)
				// Success to replicate, update matchIndex and nextIndex
				casOk := atomic.CompareAndSwapInt64(&rf.matchIndex[server], oldMatch, int64(args.Entries[len(args.Entries)-1].Index))
				if casOk {
					go rf.checkCommit()
					casOk = atomic.CompareAndSwapInt64(&rf.nextIndex[server], oldNext, int64(args.Entries[len(args.Entries)-1].Index+1))
				}
				//rf.nextIndex[server] = MaxInt(logIndex+1, rf.nextIndex[server])
				//rf.matchIndex[server] = MaxInt(logIndex, rf.matchIndex[server])
				//log.Printf(rf.toString()+" success to send logs(%d-%d) to server(%d)", args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index, server)
			}
		} else {
			//rf.sendLogTo(logIndex, server)
			//log.Printf(rf.toString() + " fail to send logs, RPC fail")
		}
	}()
}

func (log1 *LogEntry) compare(log2 *LogEntry) int {
	return compareLog(log1.Term, log2.Term, log1.Index, log2.Index)
}
func compareLog(term1, term2, index1, index2 int) int {
	if term1 != term2 {
		return term1 - term2
	}
	return index1 - index2
}

//func (log1 *LogEntry) compareTo(log2 *LogEntry) int {
//	if log1.LeaderTerm != log2.LeaderTerm {
//		return log1.LeaderTerm - log2.LeaderTerm
//	}
//	return log1.Index - log2.Index
//}

//func (rf *Raft) getLastLog() *LogEntry {
//	if rf.log == nil || len(rf.log) == 0 {
//		return nil
//	}
//	return rf.log[len(rf.log)-1]
//}

//func (rf *Raft) appendLogs(entries *LogEntry) {
//	for _, entry := range entries {
//		rf.log[i]
//	}
//}

// If RPC request or response contains LeaderTerm T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) afterRPC(term int) {
	if term > rf.currentTerm {
		log.Printf(rf.toString()+" term increased to %d\n", term)
		rf.state = FOLLOWER
		rf.currentTerm = term
		// voteFor = candidateId  in current term, so when currentTerm changed, voteFor need set null
		rf.voteFor = -1
	}
}

func (rf *Raft) afterWin() {
	rf.state = LEADER
	rf.sendHeartBeats()
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	for i := range rf.nextIndex {
		rf.nextIndex[i] = int64(len(rf.log))
	}
	rf.voteFor = -1
}

// if this peer hasn't received heartbeats recently.

func (rf *Raft) ticker() {
	// heartBeats < election out < election time out
	//go rf.sendHeartBeats()
	//go rf.checkHeartBeat()
	//go rf.applyEntry()
	for !rf.killed() {
		if !rf.isLeader() {
			rf.checkHeartBeat()
		} else {
			time.Sleep(BROADCAST_TIME)
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	if rf.isLeader() {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.sendHeartBeatTo(i)
			}
		}
	}
}

func (rf *Raft) checkHeartBeat() {
	for rf.state == FOLLOWER {
		if !rf.isHeartBeat() {
			log.Printf(rf.toString() + " start the election...")
			rf.election()
		}
	}
}

// For leader after receive cmd in Start
func (rf *Raft) replicateLog() {
	if rf.isLeader() {
		for i, index := range rf.nextIndex {
			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			log.Printf(rf.toString()+" replicate log(%d) to server(%d)...", index, i)
			if i != rf.me && int64(len(rf.log)-1) >= index {
				rf.sendLogTo(int(index), i)
			}
		}
	}

}

// For leader after sendLogTo and matchIndex changed
func (rf *Raft) checkCommit() {
	if rf.isLeader() {
		log.Printf(rf.toString() + " update commitIndex...")
		// The next index which need to be committed is the rf.commitIndex(as j) + 1
		// if matchIndex[i] > j, it means the logs which index between [j+1,matchIndex[i]] are replicated to server i
		count := 0
		targetIndex := rf.commitIndex + 1
		for _, index := range rf.matchIndex {
			if index >= targetIndex {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			oldIndex := rf.commitIndex
			if targetIndex > rf.commitIndex {
				//rf.commitIndex = targetIndex
				ok := atomic.CompareAndSwapInt64(&rf.commitIndex, oldIndex, targetIndex)
				if ok {
					go rf.applyEntry()
					log.Printf(rf.toString()+" commitIndex %d->%d, because replicated to majority", rf.commitIndex, targetIndex)
				}
			}
		}
	}

}

// For all server after commitIndex changed
func (rf *Raft) applyEntry() {
	for i := rf.lastAppliedIndex + 1; i <= int(rf.commitIndex); i++ {
		rf.applyCh <- ApplyMsg{Command: rf.log[i].Command, CommandValid: true, CommandIndex: int(i)}
		log.Printf(rf.toString()+" applied log(%d)...\n", i)
		rf.lastAppliedIndex = i
	}
}

func (rf *Raft) commitLog(index int) {
	//...do commit

}

// The ticker go routine starts a new election

func (rf *Raft) election() {
	// if server vote for other server with same term or with larger term

	rf.currentTerm++
	rf.voteFor = int64(rf.me)
	//rf.voteForTerm = rf.currentTerm
	rf.voteCount = 1
	rf.state = CANDIDATE
	// issues RequestVote RPCs in parallel to other servers
	//log.Printf("Server(id=%d, item=%d) satrt the election\n", rf.me, rf.currentTerm)

	for i := 0; i < len(rf.peers) && rf.state == CANDIDATE; i++ {
		if i != rf.me {
			rf.getVoteFrom(i)
		}
	}
	// 等待3个事件之一发生，选举就可结束
	eventCh := make(chan int)
	// 1. Win majority
	go func() {
		for int(rf.voteCount) <= len(rf.peers)/2 {
			//time.Sleep(BROADCAST_TIME)
		}
		//rf.indexMu.Lock()
		if rf.state == CANDIDATE {
			eventCh <- 0
		}
		//rf.indexMu.Unlock()
	}()

	// 2. Another server win
	go func() {
		// 1. if there is already a leader, stop election
		// 2. if there is already a candidate with higher term, stop election
		for !rf.isHeartBeat() && rf.state == CANDIDATE {
		}
		eventCh <- 1
	}()
	// 3. Time out and no winner
	go func() {
		time.Sleep(getRandTime(MIN_ElECTION_MS, MAX_ElECTION_MS))
		//rf.indexMu.Lock() // make sure the winner is turn from candidate
		if rf.state == CANDIDATE {
			eventCh <- 2
		}
		//rf.indexMu.Unlock()
	}()

	res := <-eventCh
	switch res {
	case 0:
		// make sure the winner is turn from candidate
		rf.afterWin()
		log.Println(rf.toString() + " win the election")
	case 1:
		log.Println(rf.toString() + " lost the election")
	case 2:
		// make sure the winner is turn from candidate
		//log.Printf(rf.toString() + " election time out, start to next election...")
		log.Printf(rf.toString()+" start the next election... voteCount=%d", rf.voteCount)
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
	// Create a background goroutine that will kick off leader election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	go rf.ticker()

	return rf
}

func initRaft(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		logMu:       sync.Mutex{},
		peers:       peers,
		me:          me,
		persister:   persister,
		dead:        0,
		currentTerm: 0,
		log:         []*LogEntry{}, // log entries from client or leader
		voteFor:     -1,            // means null
		//voteForTerm:      -1,
		lastHeartbeat:    time.Now(),
		commitIndex:      0,
		lastAppliedIndex: 0,
		commitMu:         sync.Mutex{},
		nextIndex:        make([]int64, len(peers)),
		matchIndex:       make([]int64, len(peers)),
		state:            FOLLOWER,
		voteCount:        0,
		applyCh:          applyCh,
	}
	// the log with index = 0 is invalid
	rf.log = append(rf.log, &LogEntry{})
	return rf
}
