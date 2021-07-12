package mr

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func NewWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	worker := &Worker{
		id: newId(),
	}

	ticker := time.NewTicker(Timeout)
	defer ticker.Stop()

	for {

		worker.askTask()
		time.Sleep(AskTaskInterval)
	}

}

type Worker struct {
	id       string
	taskType int
	state    int

	inputFile                string   // for map task input
	intermediateFilePathList []string // for map task outputs or reduce task inputs
	outputFile               string   // for reduce task output
}

func (w *Worker) askTask() error {
	args := AskTaskArgs{id: w.id}
	reply := AskTaskReply{}

	// send the RPC request, wait for the reply.
	w.call(RpcAskTask, &args, &reply)

	err := reply.err
	if err != nil {
		fmt.Printf("Worker:[%v] encounter err:[%v]", w, err)
		if errors.Is(err, ErrConflictWorkerId) {
			w.id = newId()
			return err
		}
	}

	w.taskType = reply.taskType
	w.inputFile = reply.inputFile
	w.intermediateFilePathList = reply.intermediateFilePathList

	return nil
}

func newId() string {
	return strconv.Itoa(rand.Intn(10))
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func (w *Worker) call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Printf("Worker:[%v] dialing err:[%v]", w, err)
		return err
	}
	defer c.Close()

	if err = c.Call(rpcname, args, reply); err != nil {
		fmt.Printf("Worker:[%v], call err:[%v]", w, err)
		return err
	}

	return nil
}
