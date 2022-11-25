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
	"math"

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
	BROADCAST_TIME  = 50 * TIME_UNIT   // 一个服务器将RPC并行发给集群中所有服务器（不包括其自身）并收到响应的平均时间，由机器本身特性决定
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
	Index   int64
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
	lastAppliedIndex int64 //Index of the highest log entry applied to state machine
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

func (rf *Raft) getLastIndex() int64 {
	return int64(len(rf.log) - 1)
}

// call after follower receive leader's logs or leader success to send log
func (rf *Raft) updateCommit(newCommit int64) bool {
	oldIndex := rf.commitIndex
	targetIndex := MinInt64(newCommit, rf.getLastIndex())
	var ok bool
	// If
	if targetIndex > oldIndex {
		ok = atomic.CompareAndSwapInt64(&rf.commitIndex, oldIndex, targetIndex)
		if !ok {
			log.Println("Cas fail")
		}
	}
	if ok {
		go rf.applyEntry()
	}
	return ok
}

func (rf *Raft) isHeartBeat() bool {
	randTime := getRandTime(MIN_ElECTION_MS, MAX_ElECTION_MS)
	time.Sleep(randTime)
	nowTime := time.Now()
	if nowTime.Sub(rf.lastHeartbeat) > randTime && !rf.isLeader() {
		//fmt.Printf(rf.toString()+" timeout=%v, nowTime=%v\n", randTime, nowTime.Format("15:04:05.00"))
		return false
	}
	//log.Printf(rf.toString() + " received heartbeat...")
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
	Term         int   //candidate's Term
	CandidateId  int   //candidate requesting vote
	LastLogIndex int64 //Index of candidate's last committed log entry($5.4)
	LastLogTerm  int   //Term of candidate's last committed log entry(5.4)
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
	LeaderTerm        int         // leader's LeaderTerm
	LeaderId          int         // so follower can redirect clients
	PrevLogIndex      int64       // Index of log entry immediately preceding new ones, is always equals with nextIndex-1
	PrevLogTerm       int         // LeaderTerm of prevLogIndex entry
	Entries           []*LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIndex int64       // leader's commitIndex

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
	if !rf.isLeader() {
		return 0, rf.currentTerm, false
	}
	rf.logMu.Lock()
	if !rf.isLeader() {
		return 0, rf.currentTerm, false
	}
	// make sure the visibility of rf.state
	entry := &LogEntry{rf.currentTerm, rf.getLastIndex() + 1, command}
	rf.log = append(rf.log, entry)
	log.Printf(rf.toString()+" append new log(%d), content: %v", rf.getLastIndex(), command)
	rf.matchIndex[rf.me] = entry.Index
	rf.logMu.Unlock()
	return int(entry.Index), rf.currentTerm, rf.isLeader()
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
	reply.Term = MaxInt(rf.currentTerm, args.Term)
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm || (rf.isLeader() && args.Term <= rf.currentTerm) {
		return
	}
	rf.voteMu.Lock()
	defer rf.voteMu.Unlock()
	// 在这里调用afterRPC，是因为如果rf的term低，则清除其votefor
	rf.afterRPC(args.Term)
	// 2. If votedFor is null or candidateId and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	lastLog := rf.log[rf.getLastIndex()]
	if rf.voteFor == -1 &&
		compareLog(lastLog.Term, args.LastLogTerm, lastLog.Index, args.LastLogIndex) <= 0 {
		// If a candidate can vote for another candidate, it means the latter one's term is higher
		// and the election in lower term should be stopped immediately
		// So, we treat this vote as a heartbeat to reduce election competition
		// and then this candidate will turn into a follower in afterRPC
		// args.Term must > rf.currentTerm, rf need to terminate election
		//if rf.state == CANDIDATE {
		rf.lastHeartbeat = time.Now()
		//}
		ok := atomic.CompareAndSwapInt64(&rf.voteFor, -1, int64(args.CandidateId))
		if ok {
			reply.VoteGranted, reply.Term = true, args.Term
			log.Printf(rf.toString()+" vote for (id=%d, term=%d)", args.CandidateId, args.Term)
		}
	} else {
		if args.Term < rf.currentTerm {
			log.Printf(rf.toString()+" deny vote for server(id=%d, term=%d), because candidate's term is lower", args.CandidateId, args.Term)
		} else if compareLog(lastLog.Term, args.LastLogTerm, lastLog.Index, args.LastLogIndex) > 0 {
			log.Printf(rf.toString()+" deny vote for server(id=%d, term=%d), because candidate's last log outdate", args.CandidateId, args.Term)
		} else {
			log.Printf(rf.toString()+" deny vote for server(id=%d, term=%d), because already vote for %d", args.CandidateId, args.Term, rf.voteFor)
		}

	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = MaxInt(rf.currentTerm, args.LeaderTerm)
	// 1. Reply false if term < currentTerm
	if args.LeaderTerm < rf.currentTerm || (rf.isLeader() && args.LeaderTerm == rf.currentTerm) {
		log.Printf(rf.toString()+" deny AppendEntries from server(id=%d, term=%d), because lower term", args.LeaderId, args.LeaderTerm)
		return
	}
	rf.afterRPC(args.LeaderTerm)
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex <= rf.getLastIndex() && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		rf.lastHeartbeat = time.Now()
		reply.Success = true
		if args.Entries != nil && len(args.Entries) > 0 { // heartbeat
			// 3. If an existing entry conflicts with a new one (same Index but different terms), delete the existing entry and all successor
			i := 0
			j := args.PrevLogIndex + 1
			for i < len(args.Entries) && j <= rf.getLastIndex() {
				if args.Entries[i].compare(rf.log[j]) == 0 {
					i++
					j++
				} else {
					rf.log = append(rf.log[:j])
					log.Printf(rf.toString()+" abondon logs(%d-%d) which inconsistent with leader(%d)", j, rf.getLastIndex(), args.LeaderId)
					break
				}
			}
			// Append any new entries not already in the log
			if i < len(args.Entries) {
				rf.log = append(rf.log, args.Entries[i:]...) // args.Entries[i] != rf.log[j]
				log.Printf(rf.toString()+" receive logs(%d-%d) from leader(%d)", args.Entries[i].Index, args.Entries[len(args.Entries)-1].Index, args.LeaderId)
			}
		}
		// If leaderCommit>commitIndex,set commitIndex=min(leaderCommit, lastLogIndex)
		if args.LeaderCommitIndex > rf.commitIndex && rf.updateCommit(args.LeaderCommitIndex) {
			log.Printf(rf.toString()+" update commit to %d, beacuse leader's commit is higher", rf.commitIndex)
		}

	} else {
		newLastIndex := MinInt64(args.PrevLogIndex-1, rf.getLastIndex())
		log.Printf(rf.toString()+" refuse logs from leader(id=%d), because inconsistent preLog(%d),  logCount %d->%d\n", args.LeaderId, args.PrevLogIndex, rf.getLastIndex(), newLastIndex)
		rf.log = rf.log[:newLastIndex+1]
		reply.Success = false
	}

}

func (rf *Raft) sendRequestVoteRPC(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//log.Printf(" (id=%d, term=%d) send request vote from server(id=%d)...", args.CandidateId, args.Term, server)
	if rf.state != CANDIDATE {
		log.Println(rf.toString() + " isn't candidate, so can't send request vote")
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		//log.Printf(" (id=%d, term=%d) requestVote RPC failed from server(id=%d)", args.CandidateId, args.Term, server)
	} else {
		//log.Printf(" (id=%d, term=%d) requestVote RPC response from server(id=%d)", args.CandidateId, args.Term, server)
	}
	rf.afterRPC(reply.Term)
	return ok
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	if !rf.isLeader() {
		//log.Println(rf.toString() + " isn't leader, so can't send AppendEntries RPC")
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//if !ok {
	//	log.Printf(rf.toString()+" fail to send AppendEntries RPC to server(id=%d), args is %v\n", server, args)
	//} else {
	//	log.Printf(rf.toString()+" success to send AppendEntries RPC to server(id=%d), args is %v\n", server, args)
	//}
	rf.afterRPC(reply.Term)
	return ok
}

func (rf *Raft) getVoteFrom(server int) {
	if rf.state != CANDIDATE || server == rf.me {
		return
	}
	go func() {
		args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.log[rf.commitIndex].Term}
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

// sendHeartBeat isn't diff with sendLog
//func (rf *Raft) sendHeartBeatTo(server int) {
//	go func() {
//		for !rf.killed() && rf.isLeader() {
//			preIndex := rf.nextIndex[server] - 1
//
//			args := &AppendEntriesArgs{rf.currentTerm, rf.me, preIndex, rf.log[preIndex].Term, nil, rf.commitIndex}
//			reply := &AppendEntriesReply{}
//			//log.Printf(rf.toString()+" send heartbear to server(%d)...", server)
//			rf.sendAppendEntriesRPC(server, args, reply)
//			time.Sleep(BROADCAST_TIME)
//		}
//	}()
//}

// logIndex is the index of first log entry
func (rf *Raft) sendLogTo(server int) {
	go func() {
		for !rf.killed() && rf.isLeader() {
			nextIndex := rf.nextIndex[server]
			// 如果nextIndex=0，说明发送nextIndex=1失败，nextIndex=1失败的原因有两种
			// 1. leader.term < follower.term, 该leader被剥夺权力
			// 2. leader的preLog不存在于follower， index=0的log是哨兵，每个server都有，这种可能不存在
			if nextIndex > rf.getLastIndex()+1 || nextIndex < 1 {
				log.Printf(rf.toString()+" has illegal nextIndex of server(%d), so deprive its leadership\n", nextIndex)
				rf.state = FOLLOWER
				break
			}
			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			if rf.getLastIndex() >= nextIndex {
				args := &AppendEntriesArgs{rf.currentTerm, rf.me, nextIndex - 1, rf.log[nextIndex-1].Term, rf.log[nextIndex:], rf.commitIndex}
				reply := &AppendEntriesReply{}
				//fmt.Printf("sendLogTo\n")
				ok := rf.sendAppendEntriesRPC(server, args, reply)
				if ok && reply.Success {
					// Success to replicate: nextIndex = last log+1, matchIndex=last log
					lastLogIndex := args.Entries[len(args.Entries)-1].Index
					oldMatch := rf.matchIndex[server]
					if atomic.CompareAndSwapInt64(&rf.nextIndex[server], nextIndex, lastLogIndex+1) &&
						oldMatch < lastLogIndex && atomic.CompareAndSwapInt64(&rf.matchIndex[server], oldMatch, lastLogIndex) {
						rf.checkCommit()
						//log.Printf(rf.toString()+" success to send logs(%d-%d) to server(%d)", args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index, server)
					}
				} else if ok {
					// Fail to replicate, nextIndex--
					if atomic.CompareAndSwapInt64(&rf.nextIndex[server], nextIndex, nextIndex-1) {
						log.Printf(rf.toString()+" preLog(%d) is inconsistent with (id=%d), decrements nextIndex to %d", args.PrevLogIndex, server, nextIndex-1)
					}
				}
			}
			time.Sleep(BROADCAST_TIME)
		}
	}()
}

func (rf *Raft) sendHeartBeatTo(server int) {
	go func() {
		for !rf.killed() && rf.isLeader() {
			nextIndex := rf.nextIndex[server]
			args := &AppendEntriesArgs{rf.currentTerm, rf.me, nextIndex - 1, rf.log[nextIndex-1].Term, nil, rf.commitIndex}
			reply := &AppendEntriesReply{}
			//fmt.Printf(rf.toString()+" sendHeartBeatTo %d\n", server)
			ok := rf.sendAppendEntriesRPC(server, args, reply)
			// PreIndex is inconsistent, nextIndex--
			if ok && !reply.Success &&
				atomic.CompareAndSwapInt64(&rf.nextIndex[server], nextIndex, nextIndex-1) {
				log.Printf(rf.toString()+" preLog(%d) is inconsistent with (id=%d), decrements nextIndex to %d", args.PrevLogIndex, server, nextIndex-1)
			}
			time.Sleep(BROADCAST_TIME)
		}
	}()
}

// If RPC request or response contains LeaderTerm T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) afterRPC(term int) {
	if term > rf.currentTerm {
		//log.Printf(rf.toString()+" term increased to %d\n", term)
		//if rf.state != FOLLOWER {
		//	log.Printf(rf.toString() + " become a follower, becasue find server with higher term.\n")
		//}
		rf.state = FOLLOWER
		rf.currentTerm = term
		// voteFor = candidateId  in current term, so when currentTerm changed, voteFor need set null
		rf.voteFor = -1
	}
}

func (rf *Raft) afterWin() {
	rf.state = LEADER
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastIndex() + 1
	}
	rf.sendHeartBeats()
	rf.voteFor = -1
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.sendLogs()
	//rf.replicateLogs()
}

// if this peer hasn't received heartbeats recently.

func (rf *Raft) ticker() {
	// heartBeats < election out < election time out
	//go rf.sendLogOrHeartBeats()
	//go rf.checkHeartBeat()
	//go rf.replicateLogs()
	//go rf.checkCommit()
	//go rf.applyEntry()
	for !rf.killed() {
		if !rf.isLeader() {
			rf.checkHeartBeat()
		} else {
			time.Sleep(BROADCAST_TIME)
		}
	}
}

// For leader to send heartbeat to other servers periodically
func (rf *Raft) sendLogs() {
	if rf.isLeader() {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.sendLogTo(i)
			}
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	if rf.isLeader() {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.sendHeartBeatTo(i)
			}
		}
	}
}

// For non-leader to check heartBeat from leader
func (rf *Raft) checkHeartBeat() {
	for rf.state == FOLLOWER {
		if !rf.isHeartBeat() {
			log.Printf(rf.toString() + " start the election...\n")
			rf.election()
		}
	}
}

//func (rf *Raft) replicateLogs() {
//	if rf.isLeader() {
//		for i := 0; i < len(rf.peers); i++ {
//			if i != rf.me {
//				rf.sendLogOrHeartBeatTo(i)
//			}
//		}
//	}
//}

// For leader replicate logs that received in Start()
//func (rf *Raft) replicateLogs() {
//	if rf.isLeader() {
//		for i := 0; i < len(rf.peers); i++ {
//			// If last log nextLogIndex ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
//			if i != rf.me && rf.getLastIndex() >= rf.nextIndex[i] {
//				rf.sendLogOrHeartBeatTo(i)
//			}
//		}
//	}
//}

// For leader update commit after sendLogOrHeartBeatTo() and then matchIndex changed
func (rf *Raft) checkCommit() {
	if rf.isLeader() {
		// The next index which need to be committed is the rf.commitIndex(as j) + 1
		// if matchIndex[i] > j, it means the logs which index between [j+1,matchIndex[i]] are replicated to server i
		count := 0
		oldCommitIndex := rf.commitIndex
		targetIndex := int64(math.MaxInt64)
		for _, index := range rf.matchIndex {
			if index > oldCommitIndex {
				targetIndex = MinInt64(targetIndex, index)
				count++
			}
		}
		if count > len(rf.peers)/2 {
			if targetIndex > oldCommitIndex {
				//rf.commitIndex = targetIndex
				if rf.updateCommit(targetIndex) {
					log.Printf(rf.toString()+" update commit to %d, because replicated to majority", targetIndex)
				}
			}
		}
		//targetIndex := int64(math.MaxInt64)
		//for _, index := range rf.matchIndex {
		//	targetIndex = MinInt64(index, targetIndex)
		//}
		//oldCommitIndex := rf.commitIndex
		//if targetIndex > oldCommitIndex {
		//	//rf.commitIndex = targetIndex
		//	if rf.updateCommit(targetIndex) {
		//		log.Printf(rf.toString()+" update commit to %d, because replicated to majority", targetIndex)
		//	}
		//}
	}

}

// For all server apply new committed logs after update of commitIndex
func (rf *Raft) applyEntry() {
	for i := rf.lastAppliedIndex + 1; i <= rf.commitIndex && rf.applyCh != nil; i++ {
		rf.applyCh <- ApplyMsg{Command: rf.log[i].Command, CommandValid: true, CommandIndex: int(i)}
		rf.lastAppliedIndex = i
	}
	log.Printf(rf.toString()+" applied log(%d)...\n", rf.lastAppliedIndex)
}

func (rf *Raft) commitLog(index int) {
	//...do commit

}

// The ticker go routine starts a new election

func (rf *Raft) election() {
	// if server vote for other server with same term or with larger term
	if rf.isLeader() {
		return
	}
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
		rf.afterWin()
		log.Printf(rf.toString() + " win the election\n")
	case 1:
		log.Println(rf.toString() + " lost the election")
	case 2:
		log.Printf(rf.toString() + " start the next election...")
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
