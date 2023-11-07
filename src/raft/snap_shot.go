package raft

import (
	"bytes"
	// "fmt"
	"time"

	"6.824/labgob"
)

type SnapshotArgs struct {
	Snapshot          []byte
	Entries           []Log
	LeaderCommit      int
	LastSnapShotTerm  int
	LastSnapShotIndex int
}

type SnapshotReply struct {
	MatchIndex int
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastSnapShotTerm,
		SnapshotIndex: args.LastSnapShotIndex,
	}
	rf.ApplyMsgChan <- msg
	rf.Logs = args.Entries // 直接用leader的内容将它全部覆盖
	rf.persist()
	rf.CommitIndex = args.LeaderCommit
	rf.LastApplied = 0
	rf.LastSnapShotIndex = args.LastSnapShotIndex
	rf.LastSnapShotTerm = args.LastSnapShotTerm
	reply.MatchIndex = rf.Logs[len(rf.Logs)-1].LogIndex
	// 别忘了在persister中保存raftstate和snapshot
	raftStateData := rf.WritePersist()
	rf.persister.SaveStateAndSnapshot(raftStateData, args.Snapshot)
	rf.CommitChan <- struct{}{}
}

func (rf *Raft) SendSnapshot(server int) {
	if rf.Role != LEADER {
		return
	}
	RTCTimer := time.NewTimer(RPCTimeout)
	chOk := make(chan bool)

	for {
		r := bytes.NewBuffer(rf.persister.snapshot)
		d := labgob.NewDecoder(r)
		var lastIncludedIndex int
		d.Decode(&lastIncludedIndex)
		if lastIncludedIndex != rf.LastSnapShotIndex {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		args := SnapshotArgs{
			Snapshot:          rf.persister.snapshot,
			Entries:           rf.Logs,
			LeaderCommit:      rf.CommitIndex,
			LastSnapShotIndex: rf.LastSnapShotIndex,
			LastSnapShotTerm:  rf.LastSnapShotTerm,
		}
		reply := SnapshotReply{}
		go func() {
			ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			chOk <- ok
		}()

		select {
		case <-rf.EndChan:
			return
		case <-RTCTimer.C:
			// fmt.Printf("leader %d send snapshot to peer %d timeout\n", rf.me, server)
			return
		case ok := <-chOk:
			if !ok {
				// fmt.Printf("leader %d send snapshot to peer %d timeout\n", rf.me, server)
				continue
			}
		}
		rf.MatchIndex[server] = reply.MatchIndex - rf.LastSnapShotIndex
		return
	}
}
