package raft

import (
	// "fmt"
	"time"
)

// AppendEntries RPC
type AppendEntries struct {
	Term         int   // leader's term
	LeaderId     int   // so followers can redirect clients
	PrevLogIndex int   // pre log's index of log to be send
	PrevLogTerm  int   // pre log's tem index
	Entries      []Log // log entries to store, empty for heartbeat
	LeaderCommit int   // leader's commitIndex
	IsHeartBeat  bool
}

type AppendEntriesReply struct {
	PeerTerm         int
	PeerLastLogTerm  int
	PeerLastLogIndex int
	AppendSuccess    bool
	MatchIndex       int // 当AppendSuccess为false时，需要matchIndex提供当前follower的真实index
}

func (rf *Raft) ResetAppendEntryTimers() {
	for i := 0; i < rf.NumServer; i++ {
		if i == rf.me {
			continue
		}
		rf.AppendEntriesTimers[i].Stop()
		rf.AppendEntriesTimers[i].Reset(0) // 立即开始发送
	}
}

func (rf *Raft) ResetAppendEntryTimer(server int) {
	rf.AppendEntriesTimers[server].Stop()
	rf.AppendEntriesTimers[server].Reset(HeartBeatTimeout)
}

// GetLastLogTermIndex返回全局的lastLogTerm和lastLogIndex
func (rf *Raft) GetLastLogTermIndex() (int, int) {
	//返回当前节点最后一个log的任期号和索引号
	lastLogTerm := rf.Logs[len(rf.Logs)-1].LogTerm
	//索引号都是从零开始的
	lastLogIndex := rf.LastSnapShotIndex + len(rf.Logs) - 1
	return lastLogTerm, lastLogIndex
}

// GetPrevLogTermIndex返回leader中记录的prevLogTerm和prevLogIndex
func (rf *Raft) GetPrevLogTermIndex(server int) (int, int) {
	prevLogTerm := rf.Logs[rf.MatchIndex[server]].LogTerm
	prevLogIndex := rf.Logs[rf.MatchIndex[server]].LogIndex
	return prevLogTerm, prevLogIndex
}

func (rf *Raft) IsCommitLogs() {
	hasCommited := false // 如果有需要commit的log，就设为true

	for logIndex := rf.CommitIndex + 1; logIndex <= len(rf.Logs)-1; logIndex++ {
		syncNum := 0
		for server := 0; server < rf.NumServer; server++ {
			if rf.MatchIndex[server] >= logIndex {
				syncNum++
			}
			if syncNum > rf.NumServer/2 {
				rf.CommitIndex = logIndex
				hasCommited = true
				break
			}
		}
		// 由于commitlog是连续的，如果出现一个没有达到commit标准的log，那么直接结束
		if rf.CommitIndex != logIndex {
			break
		}
	}
	if hasCommited && rf.Logs[rf.CommitIndex].LogTerm != rf.CurrentTerm {
		return
	}
	if hasCommited {
		rf.CommitChan <- struct{}{}
	}
}

func (rf *Raft) StartCommit() {

	if rf.CommitIndex > rf.LastApplied {
		lastSnapShotIndex := rf.LastSnapShotIndex
		for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				// 被snapshot打断后rf.logs改变了，因此需要减去rf.lastSnapShotIndex才能获取原来的log
				Command: rf.Logs[i+lastSnapShotIndex-rf.LastSnapShotIndex].Cmd,
				// CommandIndex属于全局index，被打断后i依然线性增长，但是原来的rf.lastSnapShotIndex却变了，导致突增，因此不能使用rf.lastSnapShotIndex
				CommandIndex: i + lastSnapShotIndex,
			}
			rf.ApplyMsgChan <- msg
			rf.LastApplied++
		}
	}
}

func (rf *Raft) GetEntry(server int) (AppendEntries, bool) {
	_, lastLogIndex := rf.GetLastLogTermIndex()
	if rf.MatchIndex[server] > len(rf.Logs)-1 {
		rf.MatchIndex[server] = len(rf.Logs) - 1
	}
	if rf.Logs[rf.MatchIndex[server]].LogIndex == lastLogIndex { // no log need to send
		args := AppendEntries{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.CommitIndex + rf.LastSnapShotIndex,
			IsHeartBeat:  true,
		}
		args.PrevLogTerm, args.PrevLogIndex = rf.GetPrevLogTermIndex(server)
		return args, true
	}
	args := AppendEntries{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.CommitIndex + rf.LastSnapShotIndex,
		IsHeartBeat:  false,
	}
	entries := append([]Log{}, rf.Logs[rf.MatchIndex[server]+1:]...)
	args.Entries = entries
	args.PrevLogTerm, args.PrevLogIndex = rf.GetPrevLogTermIndex(server)
	return args, false
}

func (rf *Raft) RequestEntry(args *AppendEntries, reply *AppendEntriesReply) {
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
	}
	snapIndex := rf.LastSnapShotIndex
	rf.ChangeRole(FOLLOWER)
	rf.ResetElectionTimer()
	LastLogTerm, LastLogIndex := rf.GetLastLogTermIndex()
	reply.PeerTerm = rf.CurrentTerm
	reply.PeerLastLogTerm = LastLogTerm
	reply.PeerLastLogIndex = LastLogIndex
	reply.AppendSuccess = false
	reply.MatchIndex = rf.LastSnapShotIndex
	//？为什么心跳的任期小于当前的任期，直接返回
	if args.Term < rf.CurrentTerm {
		return
	}

	/*
		append失败的两个原因：
		1. index没对上，但是term对上了，找index重新写，小改动；
		2. term没对上，找term重新写，大改动，需要重新写整个term
	*/
	// 首先需要考虑一下follower的lastSnapShotIndex
	if args.PrevLogIndex < rf.LastSnapShotIndex {
		reply.AppendSuccess = false
		reply.MatchIndex = rf.LastSnapShotIndex
		return
	} else if args.PrevLogIndex == rf.LastSnapShotIndex {
		if !args.IsHeartBeat {
			rf.Logs = append(rf.Logs[0:args.PrevLogIndex+1-snapIndex], args.Entries...)
			rf.persist()
			// fmt.Printf("peer %d's logs: %v\n", rf.me, rf.logs)
		}
		rf.CommitIndex = args.LeaderCommit - rf.LastSnapShotIndex
		rf.CommitChan <- struct{}{}
		reply.AppendSuccess = true
		reply.MatchIndex = rf.LastSnapShotIndex + len(args.Entries)

		return
	}
	if args.PrevLogIndex > LastLogIndex {
		// 属于index对不上的问题
		// 这里是最明显一个的错误，不管是不是同一个term，中间必然存在空块，
		// 需要将LastLogIndex作为matchIndex，补上更早的log
		reply.AppendSuccess = false
		reply.MatchIndex = LastLogIndex
		return
	}
	if args.PrevLogTerm == rf.Logs[args.PrevLogIndex-snapIndex].LogTerm {
		// 最正常的情况，leader找到follower与自己配对的log，
		// 然后把这个log后面的所有日志全部接上自己的，可能有覆盖也可能没有
		if !args.IsHeartBeat {
			// 如果args包含有要写入的内容，就在这里写入
			rf.Logs = append(rf.Logs[0:args.PrevLogIndex+1-snapIndex], args.Entries...)
			rf.persist()
			// fmt.Printf("peer %d's logs: %v\n", rf.me, rf.logs)
		}
		rf.CommitIndex = args.LeaderCommit - rf.LastSnapShotIndex
		rf.CommitChan <- struct{}{}
		reply.AppendSuccess = true
		reply.MatchIndex = args.PrevLogIndex + len(args.Entries)

		return
	}

	if args.PrevLogTerm == rf.Logs[args.PrevLogIndex-snapIndex].LogTerm {
		// args.PrevLog和follower对应位置的log在term上匹配商量，但是idx匹配不上，
		// 那就找到follower的上一个term的最后一个index作为matchIndex，
		// 把现在这个term全部重新写一遍
		idx := args.PrevLogIndex - rf.LastSnapShotIndex // idx是局部index，指在rf.logs中的相对位置
		for idx >= rf.CommitIndex && rf.Logs[idx].LogTerm == rf.CurrentTerm {
			idx--
		}
		reply.AppendSuccess = false
		reply.MatchIndex = idx + rf.LastSnapShotIndex
	} else {
		// 如果连term都匹配不上，直接从0开始，全部重写
		reply.AppendSuccess = false
		reply.MatchIndex = rf.LastSnapShotIndex
	}
}

func (rf *Raft) AppendEntriesToPeer(server int) {
	if rf.Role != LEADER {
		rf.ResetAppendEntryTimer(server) // 只有leader才能发送entry
		return
	}
	RTCTimer := time.NewTimer(RPCTimeout)
	chOk := make(chan bool)

	for {
		args, isheartBeat := rf.GetEntry(server)
		reply := AppendEntriesReply{}
		rf.ResetAppendEntryTimer(server)
		go func() {
			ok := rf.peers[server].Call("Raft.RequestEntry", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			chOk <- ok
		}()

		select {
		case <-rf.EndChan:
			return
		case <-RTCTimer.C:
			// fmt.Printf("leader %d send entry to %d timeout\n", rf.me, server)
			return
		case ok := <-chOk:
			if !ok {
				// fmt.Printf("leader %d send entry to %d failed\n", rf.me, server)
				continue
			}
		}

		if reply.PeerTerm > rf.CurrentTerm { // 存在问题，leader下台
			rf.ChangeRole(FOLLOWER)
			rf.ResetElectionTimer()
			return
		}

		if reply.AppendSuccess {
			rf.MatchIndex[server] = reply.MatchIndex - rf.LastSnapShotIndex
			if !isheartBeat {
				rf.IsCommitLogs()
			}
			return
		} else {
			rf.MatchIndex[server] = reply.MatchIndex - rf.LastSnapShotIndex
			if rf.MatchIndex[server] < 0 { // 需要append的follower部分，已经被leader保存为快照了
				rf.MatchIndex[server] = 0
				rf.InstallSnapshotChan[server] <- struct{}{}
				return
			}
			RTCTimer.Stop()
			RTCTimer.Reset(RPCTimeout)
			continue
		}
	}

}
