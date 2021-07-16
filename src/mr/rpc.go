package mr

import (
	"os"
	"time"
)
import "strconv"

const TaskRetryInterval = time.Second
const TaskMaxRetryCount = 10
const TaskHealthBeatsInterval = time.Second
const TaskHealthBeatsMaxRetryCount = 5
const TaskHealthBeatsMaxDelayTime = time.Second * 5

const CoordEvictUnhealthyWorkerTime = time.Second * 5

const RpcAskTask = "Coordinator.AskTask"
const RpcMapTask = "Coordinator.MapTask"
const RpcHealthBeats = "Coordinator.HealthBeats"

// task types
const (
	mapTaskType = iota + 1
	reduceTaskType
)

// worker status for state machine
const (
	idleWorker = iota + 1
	assignedWorker
	workedWorker
	repliedWorker
	lostWorker
)

type HealthBeatsArgs struct {
	Id  string
	Now time.Time
}

type HealthBeatsReply struct{}

type AskTaskArgs struct {
	Id string // ask worker identifier
}

type AskTaskReply struct {
	// for normal flow
	NReduce                  int      // reduce task count
	TaskType                 int      // task type
	InputFile                string   // for map task input
	IntermediateFilePathList []string // for map task outputs or reduce task inputs

	Err string
}

// worker肯定是完成了mapTask，否则怎么有脸reply呢，肯定是继续重复处理task
type MapTaskArgs struct {
	Id string // worker identifier

	IntermediateFilePathList []string
}

type MapTaskReply struct {
	Err error
}

const (
	ErrTaskNotReady     = "task not ready, please retry"
	ErrConflictWorkerId = "conflict worker identifier"
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
