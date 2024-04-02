package raft

import (
	"time"
)

func (rf *Raft) heartBeate(server int) {
	rf.mu.Lock()
	if rf.role == ILeader {
		args := rf.newAppendEntriesArgs(server)
		go rf.sendAppendEntries(server, args)
	}
	rf.mu.Unlock()
}

func (rf *Raft) heartBeater(server int) {
	heartBeateTimer := time.NewTimer(HeartBeateTimeout)
	defer heartBeateTimer.Stop()
	for rf.killed() == false {
		select {
		case <-rf.notifyStopCh:
			return
		case <-heartBeateTimer.C:
			rf.notify(rf.notifyHeartBeateCh[server])
		case <-rf.notifyHeartBeateCh[server]:
			rf.heartBeate(server)
			heartBeateTimer.Reset(HeartBeateTimeout)
		}
	}
}

func (rf *Raft) initHeartBeater() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.heartBeater(server)
	}
}
