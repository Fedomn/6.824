package mr

import (
	"container/list"
	"log"
	"sync"
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

type Coordinator struct {
	filePathList []string // input files

	assignTaskLock sync.Mutex
	mapTasks       list.List
	reduceTasks    list.List

	nMap    int // map worker count
	nReduce int // reduce worker count
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.assignTaskLock.Lock()
	defer c.assignTaskLock.Unlock()

	reply.NReduce = 2
	reply.TaskType = mapTask
	reply.InputFile = c.filePathList[0]

	return nil
}

func (c *Coordinator) MapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	log.Printf("Coordinator:[%v] get MapTask", c)

	log.Println(args)

	return nil
}

func (c *Coordinator) popMapTask() string {
	return ""
}

func (c *Coordinator) pushMapTask() string {
	return ""
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
		filePathList: files,
		nMap:         nReduce,
		nReduce:      nReduce,
	}

	// Your code here.

	c.server()
	return &c
}
