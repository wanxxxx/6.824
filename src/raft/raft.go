package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
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
	MIN_ElECTION_MS = 500
	MAX_ElECTION_MS = 1000
	TIME_UNIT       = time.Millisecond // todo
	BROADCAST_TIME  = 5                // 一个服务器将RPC并行发给集群中所有服务器（不包括其自身）并收到响应的平均时间，由机器本身特性决定
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// todo Your Command here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers(Updated on stable storage before responding to RPCs)
	currentTerm   int
	log           []*LogEntry // log entries from client or leader
	voteForId     int
	voteForTerm   int // 如果请求投票的term > voteForTerm, 则可以替换，并返回投票成功
	lastHeartbeat time.Time
	state         int // changed in the start and end of election and request or response rpc

	// Volatile state on all servers:
	commitIndex int //Index of the highest committed log
	//committedLog *LogEntry
	//lastApplied int32 //Index of the highest log entry applied to state machine
	lastAppliedLog *LogEntry

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int // Index of the next log entry to followers (initialized to leader last log Index+1)
	matchIndex []int // Index of highest log entry which is replicated on server

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

func (rf *Raft) getLastLogTerm() int {
	if rf.commitIndex >= 0 {
		return rf.log[rf.commitIndex].Term
	}
	return -1
}

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
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
	Term         int         //leader's Term
	LeaderId     int         //so follower can redirect clients
	PrevLogIndex int         // Index of log entry immediately preceding new ones
	PrevLogTerm  int         //Term of prevLogIndex entry
	Entries      []*LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         //leader's commitIndex

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
		stateName = "Follower"
	case 1:
		stateName = "Leader"
	case 2:
		stateName = "Candidate"
	}
	return fmt.Sprintf(stateName+"(id=%d, term=%d)", rf.me, rf.currentTerm)
}

func (rf *Raft) sendRequestVoteRPC(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	defer rf.afterRPC(reply.Term)
	if rf.state != CANDIDATE {
		//log.Println(rf.toString() + " isn't candidate, so can't request votes")
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		//log.Printf("RequestVote RPC failed! Server(id=%d, term=%d) send to server(id=%d)", args.CandidateId, args.Term, server)
	}
	return ok
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	defer rf.afterRPC(reply.Term)
	if !rf.isLeader() {
		log.Println(rf.toString() + " isn't leader, so can't send AppendEntries RPC")
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		log.Printf(rf.toString()+" fail to send AppendEntries RPC to server(id=%d), args is %v\n", server, args)
	}
	return ok
}

func (rf *Raft) getVoteFrom(server int) {
	if rf.state != CANDIDATE || server == rf.me {
		return
	}
	go func() {
		args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.getLastLogTerm()}
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVoteRPC(server, args, reply)
		if ok && reply.VoteGranted && rf.state == CANDIDATE {
			atomic.AddInt32(&rf.voteCount, 1)
			log.Printf(rf.toString()+" get one vote from server(id=%d), votecount=%d\n", server, rf.voteCount)
		}
	}()
}

func (rf *Raft) sendHeartBeatTo(server int) {
	if !rf.isLeader() || rf.me == server {
		return
	}
	go func() {
		args := &AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, nil, rf.commitIndex}
		reply := &AppendEntriesReply{}
		rf.sendAppendEntriesRPC(server, args, reply)
	}()
}

// logIndex is the index of first log entry
func (rf *Raft) sendLogTo(logIndex, server int) {
	if rf.me == server {
		return
	}
	go func() {
		args := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{}
		args.LeaderId = rf.me
		for logIndex >= 0 && rf.isLeader() {
			args.Term, args.PrevLogTerm, args.PrevLogIndex = rf.currentTerm, rf.getLastLogTerm(), rf.commitIndex
			args.Entries = rf.log[logIndex:]
			args.LeaderCommit = rf.commitIndex
			ok := rf.sendAppendEntriesRPC(server, args, reply)
			if !ok {
				break
			} else if !reply.Success {
				logIndex--
			} else {
				log.Printf("(id=%d, term=%d) success to send logs(index from %d to %d) to server(id=%d), args is '%v'\n...",
					rf.me, rf.currentTerm, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index, server, args)
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
				break
			}

		}
	}()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.afterRPC(args.Term)
	// todo Your code here (2A, 2B).
	// args.Term == rf.voteForTerm，则表明当前server已投票给其它相同term的candidate
	reply.Term = MaxInt(args.Term, rf.currentTerm)
	lastLog := rf.getLastLog()
	if args.Term >= rf.currentTerm &&
		args.Term > rf.voteForTerm &&
		(lastLog == nil || compareLog(lastLog.Term, lastLog.Index, args.Term, args.LastLogIndex) <= 0) {
		// 如果收到投票请求，说明leader宕机了，rf大概率在选举超时前收不到新leader的心跳
		// 投票后，term会增加，很有可能rf会启动新一轮的选举
		// 所以这里将RequestVote也看做心跳
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted, reply.Term = true, args.Term
		rf.voteForId, rf.voteForTerm = args.CandidateId, args.Term
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.afterRPC(args.Term)
	reply.Term = MaxInt(args.Term, rf.currentTerm)
	if args.Term >= rf.currentTerm {
		// heartbeat with empty log
		if args.Entries == nil || len(args.Entries) == 0 {
			rf.lastHeartbeat = time.Now()
			reply.Success = true
			//log.Printf(rf.toString()+" receive heartbeat from leader(id=%d, term=%d)\n", args.LeaderId, args.Term)
		} else if args.PrevLogIndex < 0 ||
			rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			/*
				3. If an existing entry conflicts with a new one (same Index
				but different terms), delete the existing entry and all that
				follow it($5.3)
				4. Append any new entries not already in the log
				5.If leaderCommit>commitIndex,set commitIndex=
				min(leaderCommit,Index of last new entry)
			*/
			reply.Success = true
			log.Printf(rf.toString()+" receive logs from leader(id=%d, term=%d)\n", args.LeaderId, args.Term)
		}
	}
}

// If RPC request or response contains Term T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) afterRPC(term int) {
	if term > rf.currentTerm {
		//log.Printf(rf.toString()+" term increased to %d\n", term)
		rf.state = FOLLOWER
		rf.currentTerm = term
	}
}

func compareLog(term1, term2, index1, index2 int) int {
	if term1 != term2 {
		return term1 - term2
	}
	return index1 - index2
}

//func (log1 *LogEntry) compareTo(log2 *LogEntry) int {
//	if log1.Term != log2.Term {
//		return log1.Term - log2.Term
//	}
//	return log1.Index - log2.Index
//}

func (rf *Raft) getLastLog() *LogEntry {
	if rf.log == nil || len(rf.log) == 0 {
		return nil
	}
	return rf.log[len(rf.log)-1]
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// todo Your code here (2B).
	rf.mu.Lock() // make sure the visibility of rf.state
	if !rf.isLeader() {
		return -1, -1, false
	}
	// append entry
	entry := &LogEntry{rf.currentTerm, len(rf.log), command}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = entry.Index
	rf.mu.Unlock()
	// send replica
	go rf.replicateLog(entry.Index)
	return entry.Index, rf.currentTerm, rf.isLeader()
}

// replicateLog
//  @Description: append it and send to other servers, then commit and apply
//  @receiver rf
//  @param command
//
func (rf *Raft) replicateLog(logIndex int) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendLogTo(logIndex, i)
	}

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

func (rf *Raft) checkHeartBeats() {
	for !rf.killed() {
		randTime := getRandTime(MIN_ElECTION_MS, MAX_ElECTION_MS)
		if rf.state == FOLLOWER {
			time.Sleep(randTime)
			if time.Now().Sub(rf.lastHeartbeat) > randTime {
				//log.Printf(rf.toString()+" election time out(%v), start the election...", randTime)
				rf.election()
			}
		} else {
			time.Sleep(randTime)
		}
	}
}
func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {
		if rf.isLeader() {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.sendHeartBeatTo(i)
			}
		}
		time.Sleep(getRandTime(0, BROADCAST_TIME))
	}
}

func (rf *Raft) checkCommit() {
	for !rf.killed() {
		if rf.isLeader() && rf.commitIndex >= 0 {
			count := 0
			// The next index which need to be committed is the rf.commitIndex(as j) + 1
			// if matchIndex[i] > j, it means the logs which index between [j+1,matchIndex[i]] are replicated to server i
			for _, index := range rf.matchIndex {
				if index > rf.commitIndex {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitLog(rf.commitIndex)
			}
		}
		time.Sleep(getRandTime(0, BROADCAST_TIME))
	}
}

func (rf *Raft) commitLog(index int) {
	//...do commit
	rf.commitIndex = index
	rf.applyCh <- ApplyMsg{Command: rf.log[index].Command}
}

func (rf *Raft) applyEntry() {
	for !rf.killed() {
		if rf.isLeader() {
			// applyMsg := <-rf.applyCh
		}
		time.Sleep(getRandTime(0, MIN_ElECTION_MS))
	}
}

// The ticker go routine starts a new election
// if this peer hasn't received heartbeats recently.

func (rf *Raft) ticker() {
	// heartBeats < election out < election time out
	go rf.sendHeartBeats()
	go rf.checkHeartBeats()
	go rf.checkCommit()
	go rf.applyEntry()
}

func (rf *Raft) election() {
	// if server vote for other server with same term or with larger term
	log.Printf("(id=%d, term=%d) start the election...", rf.me, rf.currentTerm)
	rf.currentTerm++
	rf.voteForId = rf.me
	rf.voteForTerm = rf.currentTerm
	rf.voteCount = 1
	rf.state = CANDIDATE
	// issues RequestVote RPCs in parallel to other servers
	//log.Printf("Server(id=%d, item=%d) satrt the election\n", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.getVoteFrom(i)
	}
	// 等待3个事件之一发生，选举就可结束
	eventCh := make(chan int)
	// 1. Win majority
	go func() {
		for int(rf.voteCount) <= len(rf.peers)/2 {
			//time.Sleep(BROADCAST_TIME)
		}
		rf.mu.Lock() // make sure the winner is turn from candidate
		defer rf.mu.Unlock()
		if rf.state == FOLLOWER {
			eventCh <- 1
		} else {
			rf.state = LEADER
			eventCh <- 0
		}
	}()

	// 2. Another server win
	go func() {
		// 1. 收到了term更高或等于的AppendEntries，因为AE只能由leader发送，变成follower
		// 2. 收到了term更到的candidate，说明下一届election已经开始，变成follower
		for rf.state != FOLLOWER {
			//time.Sleep(BROADCAST_TIME)
		}
		eventCh <- 1
	}()
	// 3. Time out and no winner
	go func() {
		time.Sleep(getRandTime(MIN_ElECTION_MS, MAX_ElECTION_MS))
		rf.mu.Lock() // make sure the winner is a candidate
		defer rf.mu.Unlock()
		if rf.state == FOLLOWER {
			eventCh <- 1
		} else {
			eventCh <- 2
		}
	}()

	res := <-eventCh
	switch res {
	case 0:
		log.Println(rf.toString() + " win the election")
	case 1:
		log.Println(rf.toString() + " lost the election")
	case 2:
		//log.Printf(rf.toString() + " election time out, start to next election...")
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
		mu:             sync.Mutex{},
		peers:          peers,
		me:             me,
		persister:      persister,
		dead:           0,
		currentTerm:    0,
		log:            []*LogEntry{}, // log entries from client or leader
		voteForId:      -1,
		voteForTerm:    -1,
		lastHeartbeat:  time.Now(),
		commitIndex:    -1,
		lastAppliedLog: &LogEntry{},
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		state:          FOLLOWER,
		voteCount:      0,
		applyCh:        applyCh,
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}
	return rf
}
