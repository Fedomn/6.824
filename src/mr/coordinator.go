package mr

import (
	"container/list"
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

type worker struct {
	id     string
	status int
}

type taskStatus int

const (
	freshTask taskStatus = iota + 1
	assignedTask
	finishedTask
)

type mapTask struct {
	inputFilePath      string
	associatedWorkerId string
	taskStartTime      time.Time
	status             taskStatus
}

type reduceTask struct {
	inputFilePathList  []string
	associatedWorkerId string
	taskStartTime      time.Time
	status             taskStatus
}

type Coordinator struct {
	filePathList []string // input files

	assignTaskLock sync.Mutex
	mapTasks       *list.List
	reduceTasks    *list.List

	healthBeatsLock            sync.Mutex
	healthBeatsForAssignedTask map[string]time.Time

	nMap    int // map worker count
	nReduce int // reduce worker count
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.assignTaskLock.Lock()
	defer c.assignTaskLock.Unlock()

	for elem := c.mapTasks.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(mapTask)
		// worker id conflict, please change worker id first
		if args.Id == task.associatedWorkerId {
			reply.Err = ErrConflictWorkerId
			return nil
		}

		if task.status == freshTask {
			// reply worker
			reply.NReduce = c.nReduce
			reply.TaskType = mapTaskType
			reply.InputFile = task.inputFilePath

			// set task
			elem.Value = mapTask{
				inputFilePath:      task.inputFilePath,
				associatedWorkerId: args.Id,
				taskStartTime:      time.Now(),
				status:             assignedTask,
			}

			c.healthBeatsLock.Lock()
			c.healthBeatsForAssignedTask[args.Id] = time.Now()
			c.healthBeatsLock.Unlock()
			return nil
		}
	}

	// no enough fresh task
	reply.Err = ErrTaskNotReady
	return nil
}

func (c *Coordinator) MapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	log.Println("Coordinator get MapTask reply")
	c.assignTaskLock.Lock()
	defer c.assignTaskLock.Unlock()

	for elem := c.mapTasks.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(mapTask)
		if task.associatedWorkerId == args.Id {
			// reply worker
			reply.Err = nil

			// finish map task
			elem.Value = mapTask{
				inputFilePath:      task.inputFilePath,
				associatedWorkerId: task.associatedWorkerId,
				taskStartTime:      task.taskStartTime,
				status:             finishedTask,
			}

			// add reduce task
			c.reduceTasks.PushBack(reduceTask{
				inputFilePathList:  args.IntermediateFilePathList,
				associatedWorkerId: "",
				taskStartTime:      time.Time{},
				status:             freshTask,
			})

			c.healthBeatsLock.Lock()
			delete(c.healthBeatsForAssignedTask, args.Id)
			c.healthBeatsLock.Unlock()
			c.logMapTasks()
			return nil
		}
	}

	reply.Err = errors.New("not found associated map task, maybe reassign to another worker")
	c.logMapTasks()
	return nil
}

func (c *Coordinator) logMapTasks() {
	for front := c.mapTasks.Front(); front != nil; front = front.Next() {
		task := front.Value.(mapTask)
		log.Printf("Current MapTask:[%+v]", task)
	}
}

func (c *Coordinator) HealthBeats(args *HealthBeatsArgs, reply *HealthBeatsReply) error {
	c.healthBeatsLock.Lock()
	defer c.healthBeatsLock.Unlock()

	c.healthBeatsForAssignedTask[args.Id] = args.Now
	return nil
}

func (c *Coordinator) evictUnhealthyAssignedWorker() {
	for {
		c.healthBeatsLock.Lock()

		healthMap := make(map[string]bool)
		for workerId, workerLastHealthTime := range c.healthBeatsForAssignedTask {
			workerCanMaxDelayTime := workerLastHealthTime.Add(TaskHealthBeatsMaxDelayTime)
			if time.Now().After(workerCanMaxDelayTime) {
				healthMap[workerId] = false
			} else {
				healthMap[workerId] = true
			}
		}

		log.Printf("Current assignedWoker healthMap:[%v]", healthMap)
		c.healthBeatsLock.Unlock()

		// reset assigned task to fresh
		c.assignTaskLock.Lock()
		c.assignTaskLock.Unlock()

		// for map task

		// for reduce task

		time.Sleep(CoordEvictUnhealthyWorkerTime)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		filePathList:               files,
		nMap:                       len(files),
		nReduce:                    nReduce,
		mapTasks:                   list.New(),
		reduceTasks:                list.New(),
		healthBeatsForAssignedTask: make(map[string]time.Time),
	}

	for _, filePath := range files {
		c.mapTasks.PushBack(mapTask{
			inputFilePath:      filePath,
			associatedWorkerId: "",
			taskStartTime:      time.Time{},
			status:             freshTask,
		})
	}

	c.server()
	go c.evictUnhealthyAssignedWorker()
	return &c
}
