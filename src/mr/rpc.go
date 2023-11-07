package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
//
// 询问时候的结构体
type ASKArgs struct {
	//询问的时候不需要任何的信息
}

const (
	MAP    = 0
	REDUCE = 1
	WAIT   = 2
	FINISH = 3
)

// 回复时候的结构体
type ASKReply struct {
	State      int    //0-map 1-reduce 2-wait 3-shutdown
	FileName   string //文件名
	TaskNumber int    //任务号
	NReduce    int    //reduce任务中的分区数
	NMap       int    //Map任务的总数
}

// 结束时候的参数
type FinishAgrs struct {
	State      int //同reply，用于更新Coordinator状态
	TaskNumber int
}

// 结束时候的回应
type FinishReply struct {
	State int //0-继续接受任务 1-任务全部完成，关闭worker
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
