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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
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

type Role string

const (
	ILeader    Role = "Leader"
	IFollower  Role = "Follower"
	ICandidate Role = "Candidate"
)

const (
	HeartBeateTimeout  = 150 * time.Millisecond
	MinElectionTimeout = 300 * time.Millisecond
	MaxElectionTimeout = 600 * time.Millisecond
	CheckPeriods       = 20 * time.Millisecond // check frequency
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	log           *LogCache
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	role          Role // current status
	electionTimer *Timer
	applyCh       chan ApplyMsg

	notifyStopCh   chan bool
	notifyApplyCh  chan bool
	notifyCommitCh chan bool

	notifyHeartBeateCh   []chan bool
	notifyLogReplicateCh []chan bool
}

// output raft's info
func (rf *Raft) String() string {
	return fmt.Sprintf("{[%v] role:%v, currentTerm:%v, votedFor:%v, "+
		"commitIndex:%v, lastApplied:%v, nextIndex:%v, matchIndex:%v, electionTimer:%v, "+
		"log:%+v}",
		rf.me, rf.role, rf.currentTerm, rf.votedFor,
		rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, rf.electionTimer, rf.log)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log *LogCache

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		Debug(dError, "Decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = rf.log.LastIncludedIndex
		// rf.lastApplied = rf.log.LastIncludedIndex
	}
	Debug(dPersist, "[%v]%v, %v, %v, %v", rf.me, currentTerm, votedFor, log, rf)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
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
	// Your data here (2A, 2B).
	Term         int // Candidate's Term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of Candidate's last Term?
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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

func (rf *Raft) notify(ch chan bool) {
	go func() {
		ch <- true
	}()
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.electionTimer = &Timer{}
	rf.electionTimer.reset()
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	// rf.log = makeLog
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = IFollower
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.notifyCommitCh = make(chan bool, 5)
	rf.notifyApplyCh = make(chan bool, 5)
	rf.notifyStopCh = make(chan bool, 5)
	rf.notifyHeartBeateCh = make([]chan bool, len(rf.peers))
	rf.notifyLogReplicateCh = make([]chan bool, len(rf.peers))
	for server, _ := range rf.peers {
		rf.notifyHeartBeateCh[server] = make(chan bool, 5)
		rf.notifyLogReplicateCh[server] = make(chan bool, 5)
	}
	// start ticker goroutine to start elections
	rf.initHeartBeater() // for heartBeat -- leader
	// rf.initLogReplicator() // for log replicate -- leader
	// go rf.applier() // for apply log --all
	go rf.ticker() // for election --Candidate
	// go rf.committer() // for commit log -- leader

	return rf
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// logCache
// 1. Logs[0] store LastIncludedIndex, LastIncludedIndex's LogEntry
// 2. Logs[] nerver will be nil, because the first point
// 3. last() return the last LogEntry
type LogCache struct {
	Logs              []*LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

// return index's LogEntry
func (l *LogCache) get(index int) *LogEntry {
	i := index - l.LastIncludedIndex
	if i >= len(l.Logs) || i < 0 {
		return nil
	}
	return l.Logs[i]
}

func (l *LogCache) last() *LogEntry {
	return l.Logs[len(l.Logs)-1]
}

// return logs which Index > index
func (l *LogCache) after(index int) []*LogEntry {
	copyData := make([]*LogEntry, l.last().Index-index)
	i := index - l.LastIncludedIndex
	copy(copyData, l.Logs[i+1:])
	return copyData
}
