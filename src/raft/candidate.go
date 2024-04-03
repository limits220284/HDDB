package raft

import (
	"math/rand"
	"time"
)

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	electionTimeout := rf.getRandElectionTimeout()
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// this goroutine aim to consistant to check the condition of election
		// once meet the condition, start the election goroutine
		// 1. in Leader status, normal
		// 2.1 in Follower status, check if the heartBeat overtime
		// 2.2 in Candidate status, check if the election overtime
		rf.mu.Lock()
		rf.electionTimer.elapsed()

		if rf.role == ILeader {
			rf.electionTimer.reset()
		}

		if rf.role == IFollower || rf.role == ICandidate {
			if rf.electionTimer.isTimeOut(electionTimeout) {
				rf.startElection()
				electionTimeout = rf.getRandElectionTimeout()
			}
		}
		rf.mu.Unlock()
		time.Sleep(CheckPeriods)
	}
}

func (rf *Raft) getRandElectionTimeout() time.Duration {
	randRange := MaxElectionTimeout - MinElectionTimeout
	return MinElectionTimeout + time.Duration(rand.Intn(int(randRange)))
}

func (rf *Raft) startElection() {
	Info(dInfo, "[%v] startElection, %+v", rf.me, rf)
	rf.role = ICandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.electionTimer.reset()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.last().Index,
		LastLogTerm:  rf.log.last().Term,
	}
	// votes
	grantedCount := 1
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		// send vote request
		go rf.sendRequestVote(i, args, &grantedCount)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, grantedCount *int) (ok bool, reply *RequestVoteReply) {
	reply = &RequestVoteReply{}
	Trace(dVote, "[%v]sendRequestVote [%v], %+v", rf.me, server, args)
	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// deal with the rpc response
	if ok {
		majority := len(rf.peers)/2 + 1
		// general check
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = IFollower
			rf.votedFor = -1
			rf.persist()
			return
		}
		// the term has been changed, this election is over
		// if self status is not candidate, meaning that become leader or Follower, so the election is over
		if rf.currentTerm != args.Term || rf.role != ICandidate {
			return
		}

		// deal the message response
		if reply.VoteGranted {
			*grantedCount = *grantedCount + 1
			if *grantedCount >= majority {
				rf.toLeader()
			}
		}
	}
	return ok, reply
}

// RequestVote RPC handle
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer Trace(dVote, "[%v]RequestVote from [%v], %+v, %+v", rf.me, args.CandidateId, args, reply, rf)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = IFollower
		rf.votedFor = -1
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastEntry := rf.log.last()

		if lastEntry.Term < args.LastLogTerm ||
			(lastEntry.Term == args.LastLogTerm && lastEntry.Index <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.electionTimer.reset()
			rf.persist()
		}

	}
	return
}
