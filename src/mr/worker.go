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
	w := newWorker()

	for {

		// main logic
		var err error
		for i := true; i; i = false {
			if err = w.askTask(); err != nil {
				break
			}

			if w.taskType == mapTask {
				if err = w.handleMapTask(mapf); err != nil {
					break
				}
				if err = w.replyMapTask(err); err != nil {
					break
				}
			} else if w.taskType == reduceTask {
			} else {
				log.Printf("Worker:[%s] receive unrecognized taskType", w)
			}

			// replied task and reset worker
			log.Printf("Worker:[%s] task already done, will aks new task", w)
			w.reset()
		}
		if err == nil {
			continue
		}

		// handle error cases
		if w.retryCount >= TaskMaxRetryCount {
			log.Printf("Worker:[%s] Retry times had exceed max, will stop worker", w)
			return
		}
		log.Printf("Worker:[%s] will retry after %s", w, TaskRetryInterval.String())
		time.Sleep(TaskRetryInterval)
		w.retryCount++
	}
}

type Worker struct {
	id       string
	taskType int
	status   int
	nReduce  int

	inputFile                string   // for map task input
	intermediateFilePathList []string // for map task outputs or reduce task inputs
	outputFile               string   // for reduce task output

	retryCount int // retry count

	// for internal use
	tmpFileMap map[int]*os.File // for intermediate temp file before os.rename
}

func (w *Worker) String() string {
	return fmt.Sprintf("id:%s taskType:%d status:%d retryCount:%d", w.id, w.taskType, w.status, w.retryCount)
}

func (w *Worker) askTask() error {
	// already assigned task
	if w.status >= assignedWorker {
		return nil
	}

	args := AskTaskArgs{Id: w.id}
	reply := AskTaskReply{}

	// send the RPC request, wait for the reply.
	if err := w.call(RpcAskTask, &args, &reply); err != nil {
		log.Printf("Worker:[%s] askTask err:[%v]", w, err)
		return err
	}

	if err := reply.Err; err != nil {
		log.Printf("Worker:[%s] askTask err:[%v]", w, err)
		if errors.Is(err, ErrConflictWorkerId) {
			w.id = newId()
		}
		return err
	}

	w.nReduce = reply.NReduce
	w.taskType = reply.TaskType
	w.inputFile = reply.InputFile
	w.intermediateFilePathList = reply.IntermediateFilePathList

	w.status = assignedWorker
	log.Printf("Worker:[%s] askTask done", w)
	return nil
}

func (w *Worker) handleMapTask(mapf func(string, string) []KeyValue) error {
	// already worked task
	if w.status >= workedWorker {
		return nil
	}

	filePath := w.inputFile
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Worker:[%s] handleMapTask err:[%v]", w, err)
		return err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("Worker:[%s] handleMapTask err:[%v]", w, err)
		return err
	}

	kva := mapf(filepath.Base(filePath), string(content))

	// write intermediate files
	for _, entry := range kva {
		tempFile, err := w.getIntermediateTempFile(entry.Key)
		if err != nil {
			log.Printf("Worker:[%s] getIntermediateTempFile err:[%v]", w, err)
			return err
		}
		// TODO
		log.Println(tempFile)

		encoder := json.NewEncoder(tempFile)
		if err := encoder.Encode(entry); err != nil {
			log.Printf("Worker:[%s] json encode err:[%v]", w, err)
			return err
		}
		if _, err = tempFile.WriteString("\r\n"); err != nil {
			log.Printf("Worker:[%s] writeString err:[%v]", w, err)
			return err
		}
		// TODO
		// os.rename
	}

	w.status = workedWorker
	log.Printf("Worker:[%s] handleMapTask done", w)
	return nil
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

func (w *Worker) replyMapTask(handleErr error) error {
	// already replied
	if w.status >= repliedWorker {
		return nil
	}

	var args MapTaskArgs
	if handleErr != nil {
		args = MapTaskArgs{id: w.id, taskStatus: taskErr}
	} else {
		args = MapTaskArgs{
			id:                       w.id,
			taskStatus:               taskDone,
			intermediateFilePathList: w.intermediateFilePathList,
		}
	}

	reply := MapTaskReply{}
	if err := w.call(RpcMapTask, &args, &reply); err != nil {
		log.Printf("Worker:[%s] replyMapTask err:[%v]", w, err)
		return err
	}

	if reply.err != nil {
		log.Printf("Worker:[%s] replyMapTask err:[%v]", w, reply.err)
	}

	w.status = repliedWorker
	log.Printf("Worker:[%s] replyMapTask done", w)
	return nil
}

func (w *Worker) reset() {
	w.id = newId()
	w.taskType = 0
	w.status = idleWorker
	w.nReduce = 0
	w.inputFile = ""
	w.intermediateFilePathList = []string{}
	w.outputFile = ""
	w.retryCount = 0
	w.tmpFileMap = make(map[int]*os.File)
}

func newWorker() *Worker {
	return &Worker{
		id:                       newId(),
		taskType:                 0,
		status:                   idleWorker,
		nReduce:                  0,
		inputFile:                "",
		intermediateFilePathList: []string{},
		outputFile:               "",
		retryCount:               0,
		tmpFileMap:               make(map[int]*os.File),
	}
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
		//log.Printf("Worker:[%v] dialing err:[%v]", w, err)
		return err
	}
	defer c.Close()

	if err = c.Call(rpcname, args, reply); err != nil {
		//log.Printf("Worker:[%v], call err:[%v]", w, err)
		return err
	}

	return nil
}
