package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

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

// Add your RPC definitions here.

type TaskStatus int

const (
    Idle TaskStatus = iota
    InProgress
    Completed
)

type TaskType int

const (
    MapTask TaskType = iota
    ReduceTask
)

// Task represents a single map or reduce job.
type Task struct {
    TaskID     int
    TaskType   TaskType
    InputFile  string
    NReduce    int
    ReduceID   int
    Status     TaskStatus
    WorkerID   int
    StartTime  time.Time
}

type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	Task *Task
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
