package raft

import "fmt"

// /*
// raft选举要注意的问题：
// 1. 基本分为选举协程，消息发送协程两大协程；
// 2. 执行众多RPC任务中，每一个点到点的RPC都创建一个协程，通信并行以提高速度，用通道实现协程间交互；
// 3. 必须考虑一个通信协程长期未反馈的情况，所以我们要创建RPCtimer设置一个时间上限；
// 4. 通信协程在RPC时间上限内，可以多次尝试连接；
// 5. 一个follower节点投票后，不管拒绝与否都要reset它的选举定时器，避免多个节点同时拉票。
// 6. 充分利用多协程，并行相对串行可以明显提高通信效率

// raft提交要注意的问题：
// 1. 需要commit的log是连续的，而不是分散的；
// 2. matchIndex代表当前follower已经被leader覆盖同化的最高位置;
// 3. lastAppliedIndex一般在CommitIndex前面，如果当前所有可以commit的log都提交了，二者会重合；
// 4. leader发送的log不是一条一条的，而是从rf.matchIndex[follower]+1到最后一条log的一个数组；
// 5. 只要follower有一次被leader的entry成功append，那么它的logs内容将和leader完全一样;
// 6. 心跳也应该拿来参与matchIndex[follower]的校准，如果仅仅靠不断重发log来校准，一是很慢，二是RPC负载太大，三是提前靠心跳顺便来完成校准，正式发送entry就不会校准出错了

// raft的snapshot注意事项：
// 1. snapshot的基本流程；
// 2. 由于snapshot的引入，全局index和局部index在peers之间，以及peer与service之间的传递与转换；
// */

// //
// // this is an outline of the API that raft must expose to
// // the service (or tester). see comments below for
// // each of these functions for more details.
// //
// // rf = Make(...)
// //   create a new Raft server.
// // rf.Start(command interface{}) (index, term, isleader)
// //   start agreement on a new log entry
// // rf.GetState() (term, isLeader)
// //   ask a Raft for its current term, and whether it thinks it is leader
// // ApplyMsg
// //   each time a new entry is committed to the log, each Raft peer
// //   should send an ApplyMsg to the service (or tester)
// //   in the same server.
// //

// import (
// 	//	"bytes"

// 	"math/rand"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	//	"6.824/labgob"
// 	"6.824/labrpc"
// )

// const (
// 	LEADER    = 0
// 	CANDIDATE = 1
// 	FOLLOWER  = 2
// )

// const (
// 	ElectionTimeout    = time.Millisecond * 300 // 选举
// 	ElectionTimeOffset = time.Millisecond * 300
// 	HeartBeatTimeout   = time.Millisecond * 150 // leader 发送心跳
// 	ApplyInterval      = time.Millisecond * 100 // apply log
// 	RPCTimeout         = time.Millisecond * 100
// 	MaxLockTime        = time.Millisecond * 10 // debug
// )

// // as each Raft peer becomes aware that successive log entries are
// // committed, the peer should send an ApplyMsg to the service (or
// // tester) on the same server, via the applyCh passed to Make(). set
// // CommandValid to true to indicate that the ApplyMsg contains a newly
// // committed log entry.
// //
// // in part 2D you'll want to send other kinds of messages (e.g.,
// // snapshots) on the applyCh, but set CommandValid to false for these
// // other uses.
// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int

// 	// For 2D:
// 	SnapshotValid bool
// 	Snapshot      []byte
// 	SnapshotTerm  int
// 	SnapshotIndex int
// }
// type Log struct {
// 	LogTerm  int
// 	LogIndex int
// 	Cmd      interface{} //??
// }

// // A Go object implementing a single Raft peer.
// type Raft struct {
// 	mu        sync.Mutex          // Lock to protect shared access to this peer's state
// 	peers     []*labrpc.ClientEnd // RPC end points of all peers
// 	persister *Persister          // Object to hold this peer's persisted state
// 	me        int                 // this peer's index into peers[]
// 	dead      int32               // set by Kill()

// 	//个人实现部分，遵循raft原论文
// 	CommitIndex         int // 局部index，到现在为止，应该被commit的log在leader.log的最高位置
// 	LastApplied         int // 局部index，到现在为止，已经被commit的log在leader.log的最高位置
// 	LastSnapShotIndex   int // 全局index，记录snapshot对应的最后一个全局index
// 	LastSnapShotTerm    int
// 	MatchIndex          []int // 局部index，follower与leader可重合的log，在ledaer的logs中的位置，注意是位置，不是log.index，只有没有snapshot的情况下位置和index才会一样
// 	NextIndex           []int // leader下一步要给follower发送的log的index
// 	CurrentTerm         int
// 	VotedFor            int
// 	Role                int // server's role: leader, candidate, follower
// 	NumServer           int // num of all servers
// 	Logs                []Log
// 	ApplyMsgChan        chan ApplyMsg
// 	CommitChan          chan struct{} // 如果该通道有消息，则开始提交lastAppliedIndex到CommitIndex之间的log
// 	EndChan             chan struct{} // if EndChan get sth, the raft ended
// 	InstallSnapshotChan []chan struct{}
// 	ElectionTimer       *time.Timer   // timer of ecection
// 	AppendEntriesTimers []*time.Timer // timer of send entry
// }

// // return currentTerm and whether this server believes it is the leader.
// func (rf *Raft) GetState() (int, bool) {
// 	var term int
// 	var isleader bool
// 	// Your code here (2A).
// 	term = rf.CurrentTerm
// 	isleader = (rf.Role == LEADER)
// 	return term, isleader
// }

// // save Raft's persistent state to stable storage,
// // where it can later be retrieved after a crash and restart.
// // see paper's Figure 2 for a description of what should be persistent.
// func (rf *Raft) persist() {
// 	// Your code here (2C).
// 	// Example:
// 	// w := new(bytes.Buffer)
// 	// e := labgob.NewEncoder(w)
// 	// e.Encode(rf.xxx)
// 	// e.Encode(rf.yyy)
// 	// data := w.Bytes()
// 	// rf.persister.SaveRaftState(data)
// 	// Your code here (2C).
// 	// Example:
// 	data := rf.WritePersist()
// 	rf.persister.SaveRaftState(data)
// }

// // A service wants to switch to snapshot.  Only do so if Raft hasn't
// // have more recent info since it communicate the snapshot on applyCh.
// func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

// 	// Your code here (2D).

// 	return true
// }

// // the service says it has created a snapshot that has
// // all info up to and including index. this means the
// // service no longer needs the log through (and including)
// // that index. Raft should now trim its log as much as possible.
// func (rf *Raft) Snapshot(index int, snapshot []byte) {
// 	// Your code here (2D).
// 	// 根据service的指示裁剪rf.logs，并且储存snapshot内容
// 	// 注意这里的index是全局index
// 	index = index - rf.LastSnapShotIndex
// 	// fmt.Printf("peer %d has a snapshot \n", rf.me)
// 	rf.LastSnapShotIndex = rf.Logs[index].LogIndex
// 	rf.LastSnapShotTerm = rf.Logs[index].LogTerm
// 	rf.Logs = rf.Logs[index:]
// 	rf.LastApplied = rf.LastApplied - index
// 	rf.CommitIndex = rf.CommitIndex - index

// 	for i := 0; i < rf.NumServer; i++ {
// 		rf.MatchIndex[i] = rf.MatchIndex[i] - index
// 		if rf.MatchIndex[i] <= 0 {
// 			rf.MatchIndex[i] = len(rf.Logs) - 1
// 		}
// 		// fmt.Printf("peer %d matchIndex: %d\n", i, rf.matchIndex[i])
// 	}
// 	// fmt.Printf("peer %d's logs: %v\n", rf.me, rf.logs)
// 	raftStateData := rf.WritePersist()
// 	rf.persister.SaveStateAndSnapshot(raftStateData, snapshot)

// }

// // example RequestVote RPC arguments structure.
// // field names must start with capital letters!
// type RequestVoteArgs struct {
// 	// Your data here (2A, 2B).
// 	Term         int // candidate's term
// 	CandidateId  int //  candidate's id
// 	LastLogIndex int // candidate's last log id
// 	LastLogTerm  int // candidate's last log term
// }

// // example RequestVote RPC reply structure.
// // field names must start with capital letters!
// type RequestVoteReply struct {
// 	// Your data here (2A).
// 	HasVoted bool
// 	Term     int
// }

// // example RequestVote RPC handler.
// // 某个节点接收到投票rpc
// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	// Your code here (2A, 2B).
// 	reply.Term = rf.CurrentTerm
// 	//如果发送请求的节点的任期小于我的，直接返回
// 	if args.Term < rf.CurrentTerm {
// 		reply.HasVoted = false
// 		return
// 		//如果发送请求的节点大于我的，投票，并且自己要变成follower
// 	} else if args.Term > rf.CurrentTerm {
// 		rf.CurrentTerm = args.Term
// 		rf.ChangeRole(FOLLOWER)
// 	}
// 	//得到自己最后的日志任期和索引，这部分应该不属于2a
// 	lastLogTerm, lastLogIndex := rf.GetLastLogTermIndex()
// 	//如果请求的任期小于自己的，并且如果任期相同，日志索引小于自己的，直接返回
// 	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
// 		reply.HasVoted = false
// 		return
// 	}
// 	// 获得认可
// 	reply.HasVoted = true
// 	rf.VotedFor = args.CandidateId
// 	rf.CurrentTerm = args.Term
// 	rf.ChangeRole(FOLLOWER)
// 	//重置选举时间
// 	rf.ResetElectionTimer()
// }

// func (rf *Raft) StartElection() {
// 	rf.ElectionTimer.Stop()
// 	rf.ElectionTimer.Reset(RandomElectionTime())

// 	if rf.Role == LEADER {
// 		return
// 	}

// 	rf.Role = CANDIDATE
// 	lastLogTerm, lastLogIndex := rf.GetLastLogTermIndex()
// 	args := RequestVoteArgs{
// 		Term:         rf.CurrentTerm,
// 		CandidateId:  rf.me,
// 		LastLogIndex: lastLogIndex,
// 		LastLogTerm:  lastLogTerm,
// 	}
// 	reply := RequestVoteReply{}

// 	voteCh := make(chan RequestVoteReply, rf.NumServer)
// 	for i := 0; i < rf.NumServer; i++ {
// 		if i == rf.me {
// 			continue
// 		}
// 		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
// 			RPCTimer := time.NewTimer(RPCTimeout)
// 			chReply := make(chan RequestVoteReply, 1)
// 			chOk := make(chan bool, 1)
// 			for {
// 				go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
// 					ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
// 					if !ok {
// 						time.Sleep(time.Millisecond * 10)
// 					}
// 					chReply <- reply
// 					chOk <- ok
// 				}(server, args, reply)
// 				select {
// 				case <-RPCTimer.C:
// 					reply := RequestVoteReply{Term: -1, HasVoted: false}
// 					voteCh <- reply
// 					return
// 				case ok := <-chOk:
// 					if ok { // call 收到消息
// 						reply := <-chReply
// 						voteCh <- reply
// 						return
// 					} else {
// 						// call 没有收到消息，重新发送
// 						continue
// 					}
// 				}
// 			}
// 		}(i, args, reply)
// 	}

// 	numVote := 1
// 	numNews := 1
// 	for { // 开始等待协程消息传回，并点票
// 		r := <-voteCh
// 		numNews++
// 		if r.HasVoted {
// 			numVote++
// 		}
// 		if r.Term > rf.CurrentTerm {
// 			rf.ChangeRole(FOLLOWER)
// 			return
// 		}
// 		if (numNews == rf.NumServer) || (numVote > rf.NumServer/2) || (numNews-numVote > rf.NumServer/2) {
// 			break
// 		}
// 	}

// 	if numVote > rf.NumServer/2 {
// 		rf.ChangeRole(LEADER)
// 		return
// 	} else {
// 		rf.ChangeRole(FOLLOWER)
// 		return
// 	}
// }

// // example code to send a RequestVote RPC to a server.
// // server is the index of the target server in rf.peers[].
// // expects RPC arguments in args.
// // fills in *reply with RPC reply, so caller should
// // pass &reply.
// // the types of the args and reply passed to Call() must be
// // the same as the types of the arguments declared in the
// // handler function (including whether they are pointers).
// //
// // The labrpc package simulates a lossy network, in which servers
// // may be unreachable, and in which requests and replies may be lost.
// // Call() sends a request and waits for a reply. If a reply arrives
// // within a timeout interval, Call() returns true; otherwise
// // Call() returns false. Thus Call() may not return for a while.
// // A false return can be caused by a dead server, a live server that
// // can't be reached, a lost request, or a lost reply.
// //
// // Call() is guaranteed to return (perhaps after a delay) *except* if the
// // handler function on the server side does not return.  Thus there
// // is no need to implement your own timeouts around Call().
// //
// // look at the comments in ../labrpc/labrpc.go for more details.
// //
// // if you're having trouble getting RPC to work, check that you've
// // capitalized all field names in structs passed over RPC, and
// // that the caller passes the address of the reply struct with &, not
// // the struct itself.
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

// // the service using Raft (e.g. a k/v server) wants to start
// // agreement on the next command to be appended to Raft's log. if this
// // server isn't the leader, returns false. otherwise start the
// // agreement and return immediately. there is no guarantee that this
// // command will ever be committed to the Raft log, since the leader
// // may fail or lose an election. even if the Raft instance has been killed,
// // this function should return gracefully.
// //
// // the first return value is the index that the command will appear at
// // if it's ever committed. the second return value is the current
// // term. the third return value is true if this server believes it is
// // the leader.
// func (rf *Raft) Start(command interface{}) (int, int, bool) {
// 	_, lastLogIndex := rf.GetLastLogTermIndex()
// 	term := rf.CurrentTerm
// 	index := lastLogIndex + 1
// 	isLeader := (rf.Role == LEADER)
// 	if isLeader {
// 		rf.Logs = append(rf.Logs, Log{
// 			LogTerm:  term,
// 			LogIndex: index, // index是全局index
// 			Cmd:      command,
// 		})
// 		rf.MatchIndex[rf.me] = len(rf.Logs) - 1
// 		rf.persist()
// 		rf.ResetAppendEntryTimers() // 立即向followers发送新的log
// 	}
// 	// 需要返回的index是全局index
// 	return index, term, isLeader
// }

// // the tester doesn't halt goroutines created by Raft after each test,
// // but it does call the Kill() method. your code can use killed() to
// // check whether Kill() has been called. the use of atomic avoids the
// // need for a lock.
// //
// // the issue is that long-running goroutines use memory and may chew
// // up CPU time, perhaps causing later tests to fail and generating
// // confusing debug output. any goroutine with a long-running loop
// // should call killed() to check whether it should stop.
// func (rf *Raft) Kill() {
// 	atomic.StoreInt32(&rf.dead, 1)
// 	// Your code here, if desired.
// }

// func (rf *Raft) killed() bool {
// 	z := atomic.LoadInt32(&rf.dead)
// 	return z == 1
// }

// // The ticker go routine starts a new election if this peer hasn't received
// // heartsbeats recently.
// func (rf *Raft) ticker() {
// 	for rf.killed() == false {

// 		// Your code here to check if a leader election should
// 		// be started and to randomize sleeping time using
// 		// time.Sleep().

// 	}
// }

// // the service or tester wants to create a Raft server. the ports
// // of all the Raft servers (including this one) are in peers[]. this
// // server's port is peers[me]. all the servers' peers[] arrays
// // have the same order. persister is a place for this server to
// // save its persistent state, and also initially holds the most
// // recent saved state, if any. applyCh is a channel on which the
// // tester or service expects Raft to send ApplyMsg messages.
// // Make() must return quickly, so it should start goroutines
// // for any long-running work.
// func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
// 	rf := &Raft{}
// 	rf.peers = peers
// 	rf.persister = persister
// 	rf.me = me
// 	rf.CommitIndex = 0
// 	rf.LastApplied = 0
// 	rf.CurrentTerm = 0
// 	rf.VotedFor = -1
// 	rf.LastSnapShotIndex = 0
// 	rf.dead = 0
// 	rf.Role = FOLLOWER
// 	rf.NumServer = len(peers)
// 	rf.ApplyMsgChan = applyCh
// 	rf.EndChan = make(chan struct{})
// 	rf.CommitChan = make(chan struct{}, 100)
// 	rf.InstallSnapshotChan = make([]chan struct{}, rf.NumServer)
// 	rf.MatchIndex = make([]int, rf.NumServer)
// 	rf.Logs = make([]Log, 1) // idx == 0 存放 lastSnapshot
// 	// fmt.Println("num of peer", rf.numServer)

// 	for i := 0; i < rf.NumServer; i++ {
// 		rf.InstallSnapshotChan[i] = make(chan struct{}, 100)
// 	}
// 	// Your initialization code here (2A, 2B, 2C).

// 	// initialize from state persisted before a crash
// 	rf.readPersist(persister.ReadRaftState())

// 	// start election goroutine
// 	rf.ElectionTimer = time.NewTimer(RandomElectionTime())
// 	go func() {
// 		for {
// 			select {
// 			case <-rf.EndChan:
// 				return
// 			case <-rf.ElectionTimer.C: // timer is over, start election
// 				rf.StartElection()
// 			}
// 		}

// 	}()

// 	// start send log entries
// 	rf.AppendEntriesTimers = make([]*time.Timer, len(peers))
// 	for i := 0; i < rf.NumServer; i++ {
// 		rf.AppendEntriesTimers[i] = time.NewTimer(HeartBeatTimeout)
// 	}

// 	go func() {
// 		for i := 0; i < rf.NumServer; i++ {
// 			if i == rf.me {
// 				continue
// 			}
// 			go func(server int) {
// 				for {
// 					select {
// 					case <-rf.EndChan:
// 						return
// 					case <-rf.AppendEntriesTimers[server].C:
// 						// fmt.Printf("leader %d to peer %d start append\n", rf.me, server)
// 						rf.AppendEntriesToPeer(server)
// 					}
// 				}
// 			}(i)

// 			go func(server int) {
// 				for {
// 					select {
// 					case <-rf.EndChan:
// 						return
// 					case <-rf.InstallSnapshotChan[server]:
// 						rf.SendSnapshot(server)
// 					}
// 				}
// 			}(i)
// 		}
// 	}()
// 	// commit log goroutine
// 	go func() {
// 		for {
// 			select {
// 			case <-rf.EndChan:
// 				return
// 			case <-rf.CommitChan:
// 				rf.StartCommit()
// 			}
// 		}
// 	}()

// 	return rf
// }

// func (rf *Raft) ChangeRole(role int) {
// 	switch role {
// 	case FOLLOWER:
// 		rf.Role = FOLLOWER
// 	case CANDIDATE:
// 		rf.Role = CANDIDATE
// 		rf.ResetElectionTimer()
// 		rf.VotedFor = rf.me
// 	case LEADER:
// 		rf.Role = LEADER
// 		rf.VotedFor = rf.me
// 		rf.CurrentTerm++
// 		for i := 0; i < rf.NumServer; i++ {
// 			// 成为leader后同步所有follower的matchIndex
// 			// 默认为leader.log的最后一个log的位置
// 			rf.MatchIndex[i] = len(rf.Logs) - 1
// 		}
// 		rf.ResetAppendEntryTimers() // 立即发送entry给所有peers
// 	}
// }

// // 初始化每个节点的计时器，通过选举时间和一个偏移量来确定
// func (rf *Raft) ResetElectionTimer() {
// 	rf.ElectionTimer.Stop()
// 	rf.ElectionTimer.Reset(RandomElectionTime())
// }

//	func RandomElectionTime() time.Duration {
//		r := time.Duration(rand.Int63()) % ElectionTimeOffset
//		return ElectionTimeout + r
//	}
func helloworld() {
	fmt.Println("hello world")
}
