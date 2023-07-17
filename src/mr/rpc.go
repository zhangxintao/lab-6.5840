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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	Assigned   bool
	TaskType   TaskType
	MapTask    MapTask
	ReduceTask ReduceTask
}

type FinishTaskArgs struct {
	TaskType   TaskType
	MapTask    FinishMapTaskArgs
	ReduceTask FinishReduceTaskArgs
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

type MapTask struct {
	Filename string
	BucketNo int
	Number   int
}

type ReduceTask struct {
	IntermediateFiles []string
	Number            int
}

type TaskType int64

const (
	Map TaskType = iota
	Reduce
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
