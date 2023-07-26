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

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	TaskId            int
	TaskType          TaskType
	MapFile           string
	IntermediateFiles []string
	NReduce           int
}

type FinishTaskArgs struct {
	TaskType          TaskType
	TaskId            int
	IntermediateFiles []string
}

type FinishTaskReply struct {
}

type FinishMapTaskArgs struct {
	SourceFilename    string
	Result            []KeyValue
	IntermediateFiles []string
}

type FinishReduceTaskArgs struct {
	Number int
}

type TaskType int64

const (
	Map TaskType = iota
	Reduce
	NoTask
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
