package mr

import (
	"errors"
	"os"
	"time"
)
import "strconv"

const Timeout = time.Second * 10
const AskTaskInterval = time.Second
const AskTaskMaxCount = 10

const RpcAskTask = "Coordinator.AskTask"

// task types
const (
	mapTask = iota
	reduceTask
)

// worker states
const (
	idleWorker = iota
	workingWorker
	lostWorker
)

// AskTaskArgs also as a heartbeat request
type AskTaskArgs struct {
	id string // ask worker identifier
}

type AskTaskReply struct {
	// for normal flow
	nReduce                  int      // reduce task count
	taskType                 int      // task type
	inputFile                string   // for map task input
	intermediateFilePathList []string // for map task outputs or reduce task inputs

	err error // for errors that includes heartbeat error
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
