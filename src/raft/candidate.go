package raft

import (
	"math/rand"
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	electionTimeout := rf.getRandElectinTimeout()
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
				electionTimeout = rf.getRandElectinTimeout()
			}
		}
		rf.mu.Unlock()
		time.Sleep(CheckPeriods)
	}
}

func (rf *Raft) getRandElectinTimeout() time.Duration {
	randRange := MaxElectionTimeout - MinElectionTimeout
	return MinElectionTimeout + time.Duration(rand.Intn(int(randRange)))
}

func (rf *Raft) startElection() {
	// Info(DPrintf())
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
	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Lock()
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
