package mr

import (
	"container/list"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
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

type taskStatus int

const (
	freshTask taskStatus = iota + 1
	assignedTask
	finishedTask
)

type mapTask struct {
	numOfMapTask       string
	inputFilePath      string
	associatedWorkerId string
	taskStartTime      time.Time
	taskEndTime        time.Time
	status             taskStatus
}

func newFreshMapTask(numOfMapTask, inputFilePath string) mapTask {
	return mapTask{
		numOfMapTask:  numOfMapTask,
		inputFilePath: inputFilePath,
		status:        freshTask,
	}
}

func (m *mapTask) assignTo(workerId string) {
	m.associatedWorkerId = workerId
	m.taskStartTime = time.Now()
	m.status = assignedTask
}

func (m *mapTask) finish() {
	m.taskEndTime = time.Now()
	m.status = finishedTask
}

func (m *mapTask) resetToFresh() {
	m.associatedWorkerId = ""
	m.taskStartTime = time.Time{}
	m.taskEndTime = time.Time{}
	m.status = freshTask
}

func (m mapTask) String() string {
	return fmt.Sprintf(
		"numOfMapTask:[%s] inputFilePath:[%s] associatedWorkerId:[%s] taskTime:[%s~%s] status:[%d]",
		m.numOfMapTask,
		m.inputFilePath,
		m.associatedWorkerId,
		m.taskStartTime.Format("15:04:05"),
		m.taskEndTime.Format("15:04:05"),
		m.status,
	)
}

type reduceTask struct {
	numOfReduceTask    string
	inputFilePathList  []string
	associatedWorkerId string
	taskStartTime      time.Time
	taskEndTime        time.Time
	status             taskStatus
	outputFile         string
}

func newFreshReduceTask(numOfReduceTask string, inputFilePathList []string) reduceTask {
	return reduceTask{
		numOfReduceTask:   numOfReduceTask,
		inputFilePathList: inputFilePathList,
		status:            freshTask,
	}
}

func (r *reduceTask) assignTo(workerId string) {
	r.associatedWorkerId = workerId
	r.taskStartTime = time.Now()
	r.status = assignedTask
}

func (r *reduceTask) finish(outputFile string) {
	r.taskEndTime = time.Now()
	r.status = finishedTask
	r.outputFile = outputFile
}

func (r *reduceTask) resetToFresh() {
	r.associatedWorkerId = ""
	r.taskStartTime = time.Time{}
	r.taskEndTime = time.Time{}
	r.status = freshTask
	r.outputFile = ""
}

func (r reduceTask) String() string {
	return fmt.Sprintf(
		"numOfReduceTask:[%s] inputFilePath:[%s] associatedWorkerId:[%s] taskTime:[%s~%s] status:[%d]",
		r.numOfReduceTask,
		r.inputFilePathList,
		r.associatedWorkerId,
		r.taskStartTime.Format("15:04:05"),
		r.taskEndTime.Format("15:04:05"),
		r.status,
	)
}

type Coordinator struct {
	filePathList []string // input files

	assignTaskLock           sync.Mutex
	mapTasks                 *list.List
	intermediateFilePathList []string // for storing map task intermediate files and convert these to reduceTasks
	reduceTasks              *list.List
	initReduceTasksOnce      sync.Once

	healthBeatsLock            sync.Mutex
	healthBeatsForAssignedTask map[string]time.Time

	nMap    int // map worker count
	nReduce int // reduce worker count
}

func (c *Coordinator) replyAssignedMapTaskToWorkerAndMarkHeartbeat(task mapTask, reply *AskTaskReply) {
	c.healthBeatsForAssignedTask[task.associatedWorkerId] = time.Now()

	reply.NReduce = c.nReduce
	reply.TaskType = mapTaskType
	reply.InputFile = task.inputFilePath
	reply.NumOfMapTask = task.numOfMapTask
}

func (c *Coordinator) replyAssignedReduceTaskToWorkerAndMarkHeartbeat(task reduceTask, reply *AskTaskReply) {
	c.healthBeatsForAssignedTask[task.associatedWorkerId] = time.Now()

	reply.NReduce = c.nReduce
	reply.TaskType = reduceTaskType
	reply.IntermediateFilePathList = task.inputFilePathList
	reply.NumOfReduceTask = task.numOfReduceTask
}

func (c *Coordinator) replyFinishedMapTaskToWorkerAndDeleteHeartbeat(task mapTask, reply *MapTaskReply) {
	delete(c.healthBeatsForAssignedTask, task.associatedWorkerId)
	reply.Err = ""
}

func (c *Coordinator) replyFinishedReduceTaskToWorkerAndDeleteHeartbeat(task reduceTask, reply *ReduceTaskReply) {
	delete(c.healthBeatsForAssignedTask, task.associatedWorkerId)
	reply.Err = ""
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.assignTaskLock.Lock()
	defer c.assignTaskLock.Unlock()

	finishedMapTasksCount := 0
	for elem := c.mapTasks.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(mapTask)
		// worker id conflict, please change worker id first
		if args.Id == task.associatedWorkerId {
			reply.Err = ErrConflictWorkerId
			return nil
		}

		if task.status == freshTask {
			// assign task to worker
			task.assignTo(args.Id)
			elem.Value = task

			// reply worker and mark health beats time for eviction
			c.healthBeatsLock.Lock()
			c.replyAssignedMapTaskToWorkerAndMarkHeartbeat(task, reply)
			c.healthBeatsLock.Unlock()
			return nil
		}

		if task.status == finishedTask {
			finishedMapTasksCount++
		}
	}

	if finishedMapTasksCount == c.nMap {
		c.initReduceTasksOnce.Do(func() {
			log.Printf("MapTasks already done, will assign ReduceTasks")
			// group intermediate files and generate reduceTask that files have same reduce number
			c.groupAndGenerateReduceTask()
			c.logReduceTasks()
		})
	}

	for elem := c.reduceTasks.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(reduceTask)
		// worker id conflict, please change worker id first
		if args.Id == task.associatedWorkerId {
			reply.Err = ErrConflictWorkerId
			return nil
		}

		if task.status == freshTask {
			// set task
			task.assignTo(args.Id)
			elem.Value = task

			// reply worker
			c.healthBeatsLock.Lock()
			c.replyAssignedReduceTaskToWorkerAndMarkHeartbeat(task, reply)
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
			// finish map task
			task.finish()
			elem.Value = task

			// add reduce task
			c.intermediateFilePathList = append(c.intermediateFilePathList, args.IntermediateFilePathList...)

			// reply worker
			c.healthBeatsLock.Lock()
			c.replyFinishedMapTaskToWorkerAndDeleteHeartbeat(task, reply)
			c.healthBeatsLock.Unlock()

			c.logMapTasks()
			return nil
		}
	}

	reply.Err = "not found associated map task, maybe reassign to another worker"
	c.logMapTasks()
	return nil
}

func (c *Coordinator) ReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	log.Println("Coordinator get ReduceTask reply")
	c.assignTaskLock.Lock()
	defer c.assignTaskLock.Unlock()

	for elem := c.reduceTasks.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(reduceTask)
		if task.associatedWorkerId == args.Id {
			// finish reduce task
			task.finish(args.OutputFile)
			elem.Value = task

			// reply worker
			c.healthBeatsLock.Lock()
			c.replyFinishedReduceTaskToWorkerAndDeleteHeartbeat(task, reply)
			c.healthBeatsLock.Unlock()

			c.logReduceTasks()
			return nil
		}
	}

	reply.Err = "not found associated reduce task, maybe reassign to another worker"
	c.logReduceTasks()
	return nil
}

// intermediate file name pattern: mr-instanceId-workerId-numOfReduceTask
func (c *Coordinator) groupAndGenerateReduceTask() {
	// remove duplicated intermediate files that maybe generated by evicted worker A
	// A replyMapTask before coordinator really evict it from mapTasks list
	// so distinct the intermediate file will be better to repair this edge case

	distinctMap := make(map[string]bool)
	for _, path := range c.intermediateFilePathList {
		_, ok := distinctMap[path]
		if ok {
			log.Printf("groupAndGenerateReduceTask encounter duplicated intermediate files: %v", path)
		} else {
			distinctMap[path] = true
		}
	}

	reduceNumMap := make(map[string][]string)
	for path := range distinctMap {
		basePath := filepath.Base(path)
		splitAry := strings.Split(basePath, "-")
		numOfReduceTask := splitAry[len(splitAry)-1]

		_, ok := reduceNumMap[numOfReduceTask]
		if !ok {
			reduceNumMap[numOfReduceTask] = make([]string, 0)
			reduceNumMap[numOfReduceTask] = append(reduceNumMap[numOfReduceTask], path)
		} else {
			reduceNumMap[numOfReduceTask] = append(reduceNumMap[numOfReduceTask], path)
		}
	}

	log.Printf("groupAndGenerateReduceTask: %v", reduceNumMap)

	for key, val := range reduceNumMap {
		reduceTask := newFreshReduceTask(key, val)
		c.reduceTasks.PushBack(reduceTask)
	}
}

func (c *Coordinator) logMapTasks() {
	for front := c.mapTasks.Front(); front != nil; front = front.Next() {
		task := front.Value.(mapTask)
		log.Printf("Current MapTask:[%+v]", task)
	}
}

func (c *Coordinator) logReduceTasks() {
	for front := c.reduceTasks.Front(); front != nil; front = front.Next() {
		task := front.Value.(reduceTask)
		log.Printf("Current ReduceTask:[%+v]", task)
	}
}

func (c *Coordinator) HealthBeats(args *HealthBeatsArgs, reply *HealthBeatsReply) error {
	c.healthBeatsLock.Lock()
	defer c.healthBeatsLock.Unlock()

	c.healthBeatsForAssignedTask[args.Id] = args.Now
	return nil
}

// goroutine
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

		log.Printf("Evictor: current assignedWoker healthMap:[%v]", healthMap)
		c.healthBeatsLock.Unlock()

		// reset assigned task to fresh
		c.assignTaskLock.Lock()
		for elem := c.mapTasks.Front(); elem != nil; elem = elem.Next() {
			task := elem.Value.(mapTask)
			if assignedWorkerHealthy, ok := healthMap[task.associatedWorkerId]; ok && !assignedWorkerHealthy {
				task.resetToFresh()
				elem.Value = task

				log.Printf("Evictor: evict unhealty assigned worker: %s", task.associatedWorkerId)
				c.healthBeatsLock.Lock()
				delete(c.healthBeatsForAssignedTask, task.associatedWorkerId)
				c.healthBeatsLock.Unlock()
			}
		}

		for elem := c.reduceTasks.Front(); elem != nil; elem = elem.Next() {
			task := elem.Value.(reduceTask)
			if assignedWorkerHealthy, ok := healthMap[task.associatedWorkerId]; ok && !assignedWorkerHealthy {
				task.resetToFresh()
				elem.Value = task

				log.Printf("Evictor: evict unhealty assigned worker: %s", task.associatedWorkerId)
				c.healthBeatsLock.Lock()
				delete(c.healthBeatsForAssignedTask, task.associatedWorkerId)
				c.healthBeatsLock.Unlock()
			}
		}
		c.logReduceTasks()
		c.assignTaskLock.Unlock()

		time.Sleep(CoordEvictUnhealthyWorkerTime)
	}
}

// goroutine
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

// goroutine
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.assignTaskLock.Lock()
	defer c.assignTaskLock.Unlock()

	if c.reduceTasks.Len() == 0 {
		return false
	}

	outputFiles := make([]string, 0, c.reduceTasks.Len())
	for elem := c.reduceTasks.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(reduceTask)
		if task.status != finishedTask {
			return false
		}
		outputFiles = append(outputFiles, task.outputFile)
	}

	log.Printf("All tasks already done. Output files: [%v]", outputFiles)
	return true
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

	for idx, filePath := range files {
		mapTask := newFreshMapTask(strconv.Itoa(idx), filePath)
		c.mapTasks.PushBack(mapTask)
	}

	c.server()
	go c.evictUnhealthyAssignedWorker()
	return &c
}
