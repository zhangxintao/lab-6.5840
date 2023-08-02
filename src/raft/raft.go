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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

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

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int
	timeout     time.Time
	role        Role

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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
				args.LastLogIndex >= len(rf.log)-1) {
			DLog(dTrace, rf.me, "RequestVote - vote granted.")
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			return
		} else {
			DLog(dTrace, rf.me, "RequestVote - vote not granted.")
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	funcName := "AppendEntries"
	DLog(dTrace, rf.me, "%v; args:%v", funcName, args)
	rf.timeout = getNextTimeout()
	DLog(dTrace, rf.me, "%v; timeout reset", funcName)

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
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	// only handle hearbeat for 2A
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
		LastLogIndex: len(rf.log) - 1,
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
						// initialize nextIndex and matchIndex
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}
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
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			Entries:      rf.log[rf.nextIndex[i]:],
			LeaderCommit: rf.commitIndex,
		}
		go func(server int, appendEntriesArgs AppendEntriesArgs) {
			DLog(dLeader, rf.me, "startAppendEntries - to:s%v, args:%v", server, appendEntriesArgs)
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &appendEntriesArgs, reply)
			if ok {
				rf.handleAppendEntriesReply(reply)
			}
		}(i, args)
	}
}

func (rf *Raft) handleAppendEntriesReply(reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
		DLog(dLeader, rf.me, "handleAppendEntriesReply")
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
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
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func getNextTimeout() time.Time {
	return time.Now().Add(time.Duration(1000+rand.Int63()%500) * time.Millisecond)
}
