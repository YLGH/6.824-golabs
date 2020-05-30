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

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType    string
	WorkerID    int
	MapReply    GetTaskMapReply
	ReduceReply GetTaskReduceReply
}

type GetTaskMapReply struct {
	File      string
	FileID    int
	NumReduce int
}

type GetTaskReduceReply struct {
	Files     []string
	Partition int
}

type TaskFinishedArgs struct {
	WorkerID int
}

type TaskFinishedReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
