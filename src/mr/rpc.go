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

// Add your RPC definitions here.

type TaskApplyArgs struct {
	TaskType string
}

type TaskApplyReply struct {
	Status string
	Task   Task
}

type TaskDoneArgs struct {
	Tasknum   int
	TaskType  string
	TaskToken int64
}

type TaskDoneReply struct {
	// do not need anything
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
