package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
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
	w := &Worker{
		id: newId(),
	}

	ticker := time.NewTicker(Timeout)
	defer ticker.Stop()

	for {

		if err := w.askTask(); err != nil {
			w.retryCount++
			if w.retryCount >= AskTaskMaxCount {
				log.Printf("Worker:[%v] Retry times had exceed max, will stop worker", w)
				return
			}
			log.Printf("Worker:[%v] will retry after %s", w, AskTaskInterval.String())
			time.Sleep(AskTaskInterval)
			continue
		}

		if w.taskType == mapTask {
			w.handleMapTask(mapf)
		} else if w.taskType == reduceTask {

		} else {
			log.Printf("Worker:[%v] receive unrecognized taskType", w)
		}

	}

}

type Worker struct {
	id       string
	taskType int
	state    int
	nReduce  int

	inputFile                string   // for map task input
	intermediateFilePathList []string // for map task outputs or reduce task inputs
	outputFile               string   // for reduce task output

	retryCount int // retry count

	// for internal use
	tmpFileMap map[int]*os.File // for intermediate temp file before os.rename
}

func (w *Worker) askTask() error {
	args := AskTaskArgs{id: w.id}
	reply := AskTaskReply{}

	// send the RPC request, wait for the reply.
	if err := w.call(RpcAskTask, &args, &reply); err != nil {
		log.Printf("Worker:[%v] encounter err:[%v]", w, err)
		return err
	}

	if err := reply.err; err != nil {
		log.Printf("Worker:[%v] encounter err:[%v]", w, err)
		if errors.Is(err, ErrConflictWorkerId) {
			w.id = newId()
		}
		return err
	}

	w.nReduce = reply.nReduce
	w.taskType = reply.taskType
	w.inputFile = reply.inputFile
	w.intermediateFilePathList = reply.intermediateFilePathList

	return nil
}

func (w *Worker) handleMapTask(mapf func(string, string) []KeyValue) ([]KeyValue, error) {
	filePath := w.inputFile
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Worker:[%v] mapTask readFile err:[%v]", w, err)
		return nil, err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("Worker:[%v] mapTask readFile err:[%v]", w, err)
		return nil, err
	}

	kva := mapf(filepath.Base(filePath), string(content))

	// write intermediate files
	for _, entry := range kva {
		tempFile, err := w.getIntermediateTempFile(entry.Key)
		if err != nil {
			return nil, err
		}
		encoder := json.NewEncoder(tempFile)
		if err := encoder.Encode(entry); err != nil {
			return nil, err
		}
		tempFile.WriteString("\r\n")
	}

	return kva, nil
}

func (w *Worker) getIntermediateTempFile(key string) (*os.File, error) {
	numOfReduceTask := ihash(key) % w.nReduce
	file, ok := w.tmpFileMap[numOfReduceTask]
	if ok {
		return file, nil
	}
	intermediateFileName := fmt.Sprintf("mr-%s-%d", w.id, numOfReduceTask)
	tempFile, err := ioutil.TempFile("", intermediateFileName)
	if err != nil {
		return nil, err
	}

	w.tmpFileMap[numOfReduceTask] = file
	return tempFile, nil
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
		log.Printf("Worker:[%v] dialing err:[%v]", w, err)
		return err
	}
	defer c.Close()

	if err = c.Call(rpcname, args, reply); err != nil {
		log.Printf("Worker:[%v], call err:[%v]", w, err)
		return err
	}

	return nil
}
