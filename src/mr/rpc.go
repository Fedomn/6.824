package mr

import (
	"errors"
	"os"
	"time"
)
import "strconv"

const TaskRetryInterval = time.Second
const TaskMaxRetryCount = 10

const RpcAskTask = "Coordinator.AskTask"
const RpcMapTask = "Coordinator.MapTask"

// task types
const (
	mapTask = iota + 1
	reduceTask
)

// worker status for state machine
const (
	idleWorker = iota + 1
	assignedWorker
	workedWorker
	repliedWorker
	lostWorker
)

// task status
const (
	taskDone = iota + 1
	taskErr
)

// AskTaskArgs also as a heartbeat request
type AskTaskArgs struct {
	Id string // ask worker identifier
}

type AskTaskReply struct {
	// for normal flow
	NReduce                  int      // reduce task count
	TaskType                 int      // task type
	InputFile                string   // for map task input
	IntermediateFilePathList []string // for map task outputs or reduce task inputs

	Err error // for errors that includes heartbeat error
}

type MapTaskArgs struct {
	id         string // worker identifier
	taskStatus int    // task status

	intermediateFilePathList []string
}

type MapTaskReply struct {
	err error
}

var (
	ErrTaskNotReady     = errors.New("task not ready, please retry")
	ErrConflictWorkerId = errors.New("conflict worker identifier")
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
