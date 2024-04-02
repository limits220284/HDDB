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
				rf.StartElection()
				electionTimeout = rf.getRandElectinTimeout()
			}
		}
		rf.mu.Unlock()
		time.Sleep(CheckPeriods)
	}
}

func (rf *Raft) getRandElectinTimeout() time.Duration {
	randRange := MaxEelectionTimeout - MinelectionTimeout
	return MinElectionTimeout + time.Duration(rand.Intn(int(randRange)))
}
