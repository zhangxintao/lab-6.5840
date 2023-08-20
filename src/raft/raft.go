package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type Role int64

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// persistent state on all servers
	currentTerm       int
	votedFor          int
	log               []LogEntry
	lastIncludedTerm  int
	lastIncludedIndex int

	// volatile state on all servers
	commitIndex int
	lastApplied int
	timeout     time.Time
	role        Role
	snapshot    []byte

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	DLog(dTrace, rf.me, "GetState - currentTerm:%v, role:%v", rf.currentTerm, rf.role)
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	funcName := "persist"
	DLog(dTrace, rf.me, "%v:persisting", funcName)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	funcName := "readPersist"
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DLog(dWarn, rf.me, "%v:invalid input data", funcName)
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DLog(dWarn, rf.me, "%v:no value found", funcName)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.snapshot = snapshot
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DLog(dSnap, rf.me, "snapshotting started - index:%v, log:%v", index, rf.log)

	if index <= rf.lastIncludedIndex || index > rf.lastApplied || index > rf.lastLogIndex() {
		DLog(dSnap, rf.me, "snapshotting - invalid index:%v", index)
		return
	}
	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex].Term
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	// include the log at "lastIncludedIndex" on purpose, so that the 0 position always has a valid entry with same term of lastIncludedTerm
	DLog(dSnap, rf.me, "snapshotting done - lastIncludedIndex:%v", rf.lastIncludedIndex)

	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DLog(dTrace, rf.me, "RequestVote from:%v; args:%v", args.CandidateId, args)
	if args.Term < rf.currentTerm {
		DLog(dTrace, rf.me, "RequestVote - current term is higher")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		DLog(dTrace, rf.me, "RequestVote - current term is lower")
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check if candidate's log is at least as up-to-date as receiver's log
		DLog(dTrace, rf.me, "RequestVote - checking for latest")
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
				args.LastLogIndex >= rf.lastLogIndex()) {
			DLog(dTrace, rf.me, "RequestVote - vote granted.")
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.timeout = getNextTimeout()
			return
		} else {
			DLog(dTrace, rf.me, "RequestVote - vote not granted. comparing: %v, %v, %v, %v", args.LastLogTerm, rf.log[len(rf.log)-1].Term, args.LastLogIndex, rf.lastLogIndex())
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	funcName := "AppendEntries"
	DLog(dTrace, rf.me, "%v; args len:%v", funcName, len(args.Entries))
	reply.XIndex = -1
	reply.XTerm = -1

	if args.Term < rf.currentTerm {
		DLog(dTrace, rf.me, "%v:lower term detected: %v", funcName, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		DLog(dTrace, rf.me, "%v:higher term detected: %v", funcName, args.Term)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	if rf.role == Leader {
		// in a valid raft impl, there should not any case that 2 leaders exist in a term even when one of them are not aware of it
		DLog(dError, rf.me, "%v:detected another leader:%v for term:%v", funcName, args.LeaderId, rf.currentTerm)
		return
	}

	rf.timeout = getNextTimeout()
	DLog(dTrace, rf.me, "%v; timeout reset", funcName)

	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= rf.lastLogIndex()+1 {
		DLog(dTrace, rf.me, "%v:follower's log is too short", funcName)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XIndex = rf.lastIncludedIndex + len(rf.log) - 1
		return
	}

	if args.PrevLogIndex < rf.lastIncludedIndex {
		DLog(dTrace, rf.me, "%v:prevLogIndex:%v < rf.lastIncludedIndex:%v", funcName, args.PrevLogIndex, rf.lastIncludedIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XIndex = rf.lastIncludedIndex + 1
		return
	}

	if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		DLog(dTrace, rf.me, "%v:term mismatch, prev:%v, last:%v, args.PrevTerm:%v, rf.log.term:%v", funcName, args.PrevLogIndex, rf.lastIncludedIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term)
		reply.XTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XIndex = args.PrevLogIndex
		// find the first index of the term
		for i := args.PrevLogIndex - 1; i >= rf.lastIncludedIndex; i-- {
			if rf.log[i-rf.lastIncludedIndex].Term != reply.XTerm {
				reply.XIndex = i + 1
				break
			}
		}
		return
	}

	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// check if an existing entry conflicts with a new one (same index but different terms)
	// delete the existing entry and all that follow it
	// append any new entries not already in the log
	DLog(dTrace, rf.me, "%v:log match, count of entries to append:%v", funcName, len(args.Entries))
	rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludedIndex+1]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = true

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		DLog(dTrace, rf.me, "%v:leaderCommit > commitIndex", funcName)
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > rf.lastLogIndex() {
			DLog(dTrace, rf.me, "%v:commitIndex > len(rf.log)-1", funcName)
			rf.commitIndex = rf.lastLogIndex()
		}

		// apply log entries that have not been applied
		applyMsgs := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			command := rf.log[rf.lastApplied-rf.lastIncludedIndex].Command
			DLog(dTrace, rf.me, "%v:apply log entry:%v, index:%v", funcName, command, rf.lastApplied)
			applyMsgs = append(applyMsgs, ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: rf.lastApplied,
			})
		}
		go func(msgs []ApplyMsg) {
			for _, msg := range msgs {
				rf.applyCh <- msg
			}
		}(applyMsgs)
	}
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DLog(dTrace, rf.me, "try to start agreement on the next command:%v", command)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := false
	if rf.role != Leader {
		return index, term, isLeader
	}

	// Your code here (2B).
	// append the command to the log
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	index = rf.lastLogIndex()
	term = rf.currentTerm
	isLeader = true
	DLog(dTrace, rf.me, "appending the command to log:%v, index:%v", command, index)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role == Leader {
				go rf.startAppendEntries()
				go rf.startInstallSnapshot()
			} else if rf.timeout.Before(time.Now()) {
				DLog(dTimer, rf.me, "timeout")
				go rf.startElection()
			}
		}()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// Leader guarantees that all snapshot data is applied/committed before sending new entries
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DLog(dSnap, rf.me, "received InstallSnapshot")

	rf.mu.Lock()
	defer rf.mu.Unlock()
	funcName := "InstallSnapshot"
	DLog(dSnap, rf.me, "%v; args len:%v", funcName, len(args.Data))

	if args.Term < rf.currentTerm {
		DLog(dSnap, rf.me, "%v:lower term detected: %v", funcName, args.Term)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		DLog(dSnap, rf.me, "%v:higher term detected: %v", funcName, args.Term)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	rf.timeout = getNextTimeout()
	DLog(dSnap, rf.me, "%v; timeout reset", funcName)

	if rf.commitIndex >= args.LastIncludedIndex {
		DLog(dSnap, rf.me, "%v; rf.commitIndex >= args.LastIncludedIndex", funcName)
		reply.Term = rf.currentTerm
		return
	}

	// always keep the 0 position on purpose
	if rf.lastLogIndex() > args.LastIncludedIndex {
		rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
	} else {
		rf.log = rf.log[0:1]
		rf.log[0].Term = args.LastIncludedTerm
	}
	rf.snapshot = args.Data

	// discard any existing or partial snapshot with a smaller index
	// and replace with the new snapshot
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	go func(msg ApplyMsg) {
		rf.applyCh <- applyMsg
	}(applyMsg)

	DLog(dSnap, rf.me, "%v;done.", funcName)
}

func (rf *Raft) startInstallSnapshot() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DLog(dSnap, rf.me, "startInstallSnapshot - started")

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}

	// leader to make sure the snapshot data is applied/committed.
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			DLog(dSnap, rf.me, "startInstallSnapshot - target: %v, nextIndex:%v <= lastIncludedIndex:%v", i, rf.nextIndex[i], rf.lastIncludedIndex)
			go func(server int, args InstallSnapshotArgs) {
				reply := InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(server, &args, &reply)
				if ok {
					rf.handleInstallSnapshotReply(&args, &reply, server)
				}
			}(i, args)
		}
	}

	DLog(dSnap, rf.me, "startInstallSnapshot - done")
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DLog(dSnap, rf.me, "sending InstallSnapshot to:%v", server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleInstallSnapshotReply(args *InstallSnapshotArgs, reply *InstallSnapshotReply, server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DLog(dLeader, rf.me, "handleInstallSnapshotReply started - from:%v", server)
	if rf.currentTerm != args.Term || rf.role != Leader {
		DLog(dWarn, rf.me, "handleInstallSnapshotReply - current term:%v is different from args term:%v", rf.currentTerm, args.Term)
		return
	}

	if reply.Term > rf.currentTerm {
		DLog(dSnap, rf.me, "handleInstallSnapshotReply - higer term:%v detected", reply.Term)
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	DLog(dSnap, rf.me, "handleInstallSnapshotReply done - updated nextIndex and matchIndex for server:%v", server)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DLog(dVote, rf.me, "startElection")
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = Candidate
	rf.timeout = getNextTimeout()
	rf.votedFor = rf.me
	rf.persist()
	// send RequestVote RPCs to all other servers concurrently
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	votes := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			DLog(dVote, rf.me, "starelection - requestVote; to:%v", server)
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DLog(dVote, rf.me, "startelection - requestVote - process result")
				if rf.currentTerm != args.Term || rf.role == Leader {
					DLog(dVote, rf.me, "start election - current term:%v is different from args term:%v", rf.currentTerm, args.Term)
					return
				}
				if reply.Term > rf.currentTerm {
					DLog(dVote, rf.me, "start election - requestVote to: s%v, higher term detected: %v", server, reply.Term)
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}
				if reply.VoteGranted {
					DLog(dVote, rf.me, "vote granted from s%v, current vote:%v", server, votes)
					votes++
					if votes > len(rf.peers)/2 {
						DLog(dVote, rf.me, "requestVote - get majority")
						rf.role = Leader
						// send initial empty AppendEntries RPCs (heartbeat) to each server
						go rf.startAppendEntries()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) startAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DLog(dLeader, rf.me, "startAppendEntries - starting")
	for i := 0; i < len(rf.peers); i++ {
		DLog(dLeader, rf.me, "startAppendEntries - starting, target:%v", i)
		if i == rf.me {
			continue
		}
		DLog(dLeader, rf.me, "startAppendEntries - started, target:%v", i)
		// if last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		// if successful: update nextIndex and matchIndex for follower
		// if AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		// if there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N

		if rf.lastLogIndex()+1 < rf.nextIndex[i] {
			DLog(dError, rf.me, "nextIndex:%v ahead the last log entry:%v", rf.nextIndex[i], rf.lastLogIndex())
			continue
		}

		if rf.lastIncludedIndex >= rf.nextIndex[i] {
			DLog(dLeader, rf.me, "startAppendEntries - target:%v, waiting for install snapshot finish before append new entries", i)
			continue
		}

		// start appendentries guarantees that nextIndex[i] is always valid( > lastIncludedIndex)
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1-rf.lastIncludedIndex].Term,
			Entries:      rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:],
			LeaderCommit: rf.commitIndex,
		}

		go func(server int, appendEntriesArgs AppendEntriesArgs) {
			DLog(dLeader, rf.me, "startAppendEntries - to:s%v, args len:%v", server, len(appendEntriesArgs.Entries))
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &appendEntriesArgs, reply)
			if ok {
				rf.handleAppendEntriesReply(appendEntriesArgs, reply, server)
			}
		}(i, args)
	}
	DLog(dLeader, rf.me, "startAppendEntries - done")
}

func (rf *Raft) handleAppendEntriesReply(args AppendEntriesArgs, reply *AppendEntriesReply, server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term || rf.role != Leader {
		DLog(dWarn, rf.me, "handleAppendEntriesReply - current term:%v is different from args term:%v", rf.currentTerm, args.Term)
		return
	}

	if reply.Term > rf.currentTerm {
		DLog(dLeader, rf.me, "handleAppendEntriesReply - higer term:%v detected", reply.Term)
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.Success {
		// update nextIndex and matchIndex for follower
		DLog(dLeader, rf.me, "handleAppendEntriesReply for success, prev:%v, len of entries:%v", args.PrevLogIndex, len(args.Entries))
		if rf.nextIndex[server] < args.PrevLogIndex+len(args.Entries)+1 {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			DLog(dLeader, rf.me, "handleAppendEntriesReply - updated nextIndex and matchIndex for server:%v", server)
			// if there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
			// since lastLogIndex <= lastApplied, and lastApplied <= commitIndex, so we are safe to access log based on commitIndex
			for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {
				count := 1
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N {
						count++
					}
				}
				if count > len(rf.peers)/2 && rf.log[N-rf.lastIncludedIndex].Term == rf.currentTerm {
					// commitment can only be determined by current term.
					DLog(dLeader, rf.me, "handleAppendEntriesReply - update commitIndex to:%v", N)
					rf.commitIndex = N
					break
				}
			}

			applyMsgs := make([]ApplyMsg, 0)
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				command := rf.log[rf.lastApplied-rf.lastIncludedIndex].Command
				DLog(dLeader, rf.me, "handleAppendEntriesReply - apply log entry at index:%v, command:%v", rf.lastApplied, command)
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: rf.lastApplied,
				}
				applyMsgs = append(applyMsgs, applyMsg)
			}

			go func() {
				for _, msg := range applyMsgs {
					rf.applyCh <- msg
				}
			}()

		}
	} else {
		// decrement nextIndex and retry
		DLog(dLeader, rf.me, "handleAppendEntriesReply for fail")
		rf.nextIndex[server] = reply.XIndex
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.timeout = getNextTimeout()
	rf.role = Follower
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	DLog(dTrace, rf.me, "inited")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func getNextTimeout() time.Time {
	return time.Now().Add(time.Duration(1000+rand.Int63()%500) * time.Millisecond)
}
