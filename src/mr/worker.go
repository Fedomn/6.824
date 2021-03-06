package mr

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "hash/fnv"

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	go w.heartbeat()

	for {

		// main logic
		var err error
		for i := true; i; i = false {
			if err = w.askTask(); err != nil {
				break
			}

			switch w.taskType {
			case TaskType_mapTaskType:
				if err = w.handleMapTask(mapf); err != nil {
					break
				}
				if err = w.replyMapTask(); err != nil {
					break
				}
			case TaskType_reduceTaskType:
				if err = w.handleReduceTask(reducef); err != nil {
					break
				}
				if err = w.replyReduceTask(); err != nil {
					break
				}
			default:
				log.Printf("Worker:[%s] receive unrecognized taskType:[%v]", w, w.taskType)
				err = errors.New("unrecognized taskType")
				break
			}

			// replied task and reset worker
			log.Printf("Worker:[%s] task already done, will aks new task", w)
			log.Println("-----------")

			w.lock.Lock()
			w.reset()
			w.lock.Unlock()
		}
		if err == nil {
			continue
		}

		// handle task not ready situation, do nothing
		if strings.Contains(err.Error(), ErrTaskNotReady) {
			log.Printf("Worker:[%s] will retry after %s", w, TaskRetryInterval.String())
			time.Sleep(TaskRetryInterval)
			continue
		}

		// handle error cases
		if w.errRetryCount >= TaskMaxRetryCount {
			log.Printf("Worker:[%s] Retry times had exceed max, will stop worker", w)
			return
		}
		log.Printf("Worker:[%s] will retry after %s", w, TaskRetryInterval.String())
		time.Sleep(TaskRetryInterval)
		w.errRetryCount++
	}
}

type Worker struct {
	instance string // for host instance distinguish
	id       string // for task level distinguish
	taskType TaskType
	status   WorkerStatus
	nReduce  int32

	inputFile                string   // for map task input
	numOfMapTask             string   // for map task output filename
	intermediateFilePathList []string // for map task outputs or reduce task inputs
	numOfReduceTask          string   // for reduce task output filename
	outputFile               string   // for reduce task output

	errRetryCount       int // error retry count
	heartbeatRetryCount int // heartbeat retry count

	// for internal use
	tmpFileMap map[int]*os.File // for intermediate temp file before os.rename, key is numOfReduceTask, value is filePointer

	// for data race when heartbeats
	lock sync.Mutex
}

func (w *Worker) String() string {
	return fmt.Sprintf(
		"instance:%s id:%s taskType:%d numOfMapTask:%s numOfReduceTask:%s nReduce:%d status:%d errRetryCount:%d",
		w.instance, w.id, w.taskType, w.numOfMapTask, w.numOfReduceTask, w.nReduce, w.status, w.errRetryCount)
}

func (w *Worker) askTask() error {
	// already assigned task
	if w.status >= WorkerStatus_assignedWorker {
		return nil
	}

	args := AskTaskArgs{Id: w.id}
	reply := AskTaskReply{}

	// send the RPC request, wait for the reply.
	if err := w.AskTask(&args, &reply); err != nil {
		log.Printf("Worker:[%s] askTask err:[%v]", w, err)
		return err
	}

	if err := reply.Err; err != "" {
		log.Printf("Worker:[%s] askTask err:[%v]", w, err)
		if err == ErrConflictWorkerId {
			w.lock.Lock()
			w.id = newId()
			w.lock.Unlock()
		}

		return errors.New(err)
	}

	w.lock.Lock()
	w.nReduce = reply.NReduce
	w.taskType = reply.TaskType
	w.inputFile = reply.InputFile
	w.numOfMapTask = reply.NumOfMapTask
	w.intermediateFilePathList = reply.IntermediateFilePathList
	w.numOfReduceTask = reply.NumOfReduceTask

	w.status = WorkerStatus_assignedWorker
	w.lock.Unlock()
	log.Printf("Worker:[%s] askTask done", w)
	return nil
}

/** for map task  **/
func (w *Worker) handleMapTask(mapf func(string, string) []KeyValue) error {
	// already worked task
	if w.status >= WorkerStatus_workedWorker {
		return nil
	}

	filePath := w.inputFile
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Printf("Worker:[%s] handleMapTask err:[%v]", w, err)
		return err
	}

	kva := mapf(filePath, string(content))

	// write intermediate files
	for _, entry := range kva {
		tempFile, err := w.getIntermediateTempFile(entry.Key)
		if err != nil {
			log.Printf("Worker:[%s] getIntermediateTempFile err:[%v]", w, err)
			return err
		}

		encoder := json.NewEncoder(tempFile)
		if err := encoder.Encode(entry); err != nil {
			log.Printf("Worker:[%s] json encode err:[%v]", w, err)
			return err
		}
	}

	if err := w.commitIntermediateFile(); err != nil {
		log.Printf("Worker:[%s] commitIntermediateFile err:[%v]", w, err)
		return err
	}
	//log.Printf("Worker:[%s] commit intermediate success!", w)

	w.lock.Lock()
	w.status = WorkerStatus_workedWorker
	w.lock.Unlock()
	log.Printf("Worker:[%s] handleMapTask done", w)
	return nil
}

func (w *Worker) getIntermediateTempFile(key string) (*os.File, error) {
	numOfReduceTask := ihash(key) % int(w.nReduce)
	if file, ok := w.tmpFileMap[numOfReduceTask]; ok {
		return file, nil
	}
	intermediateFileName := w.intermediateFileName(numOfReduceTask)
	tempFile, err := ioutil.TempFile("", intermediateFileName+"-*")
	if err != nil {
		return nil, err
	}

	w.tmpFileMap[numOfReduceTask] = tempFile
	return tempFile, nil
}

func (w *Worker) commitIntermediateFile() error {
	for numOfReduceTask, tempFile := range w.tmpFileMap {
		oldFilePath := tempFile.Name()
		currentDir, err := filepath.Abs("./")
		if err != nil {
			return err
		}
		newFilePath := filepath.Join(currentDir, w.intermediateFileName(numOfReduceTask))
		if err := os.Rename(oldFilePath, newFilePath); err != nil {
			return err
		}
		w.intermediateFilePathList = append(w.intermediateFilePathList, newFilePath)
	}

	// close tmpFiles
	for _, tempFile := range w.tmpFileMap {
		if err := tempFile.Close(); err != nil {
			log.Printf("Worker:[%s] close tmpFile err:[%v]", w, err)
		}
	}
	return nil
}

func (w *Worker) intermediateFileName(numOfReduceTask int) string {
	return fmt.Sprintf("mr-%s-%d", w.numOfMapTask, numOfReduceTask)
}

func (w *Worker) replyMapTask() error {
	// already replied
	if w.status >= WorkerStatus_repliedWorker {
		return nil
	}

	args := MapTaskArgs{
		Id:                       w.id,
		IntermediateFilePathList: w.intermediateFilePathList,
	}

	reply := MapTaskReply{}
	if err := w.MapTask(&args, &reply); err != nil {
		log.Printf("Worker:[%s] replyMapTask err:[%v]", w, err)
		return err
	}

	if reply.Err != "" {
		log.Printf("Worker:[%s] replyMapTask err:[%v]", w, reply.Err)
		// ??????worker??????????????????????????????reply??????coord, ??????coord reply???err???worker?????????????????????????????????????????????
	}
	w.lock.Lock()
	w.status = WorkerStatus_repliedWorker
	w.lock.Unlock()
	log.Printf("Worker:[%s] replyMapTask done", w)
	return nil
}

/** for reduce task  **/
func (w *Worker) handleReduceTask(reducef func(string, []string) string) error {
	// already worked task
	if w.status >= WorkerStatus_workedWorker {
		return nil
	}

	// read all file into memory
	// sort all KeyValue
	intermediateKV := make([]KeyValue, 0)

	for _, filePath := range w.intermediateFilePathList {
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("Worker:[%s] handleReduceTask err:[%v]", w, err)
			return err
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			kv := KeyValue{}
			text := scanner.Text()
			decoder := json.NewDecoder(strings.NewReader(text))
			if err := decoder.Decode(&kv); err != nil {
				log.Printf("Worker:[%s] handleReduceTask decode err:[%v]", w, err)
			} else {
				intermediateKV = append(intermediateKV, kv)
			}
		}
	}

	sort.Sort(ByKey(intermediateKV))

	// generate output file
	outputFileName := w.outputFileName(w.numOfReduceTask)
	tempFile, err := ioutil.TempFile("", outputFileName)
	if err != nil {
		log.Printf("Worker:[%s] handleReduceTask gen tempFile err:[%v]", w, err)
		return err
	}

	// call Reduce on each distinct key in intermediateKV and print the result to outputFile
	i := 0
	for i < len(intermediateKV) {
		// ??????i???j??????????????????????????????0??????1??????????????????????????????
		j := i + 1
		// ??????????????????????????????????????????????????????i?????????key ?????????
		for j < len(intermediateKV) && intermediateKV[j].Key == intermediateKV[i].Key {
			j++
		}
		// ??????????????????????????? copy ???values???
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediateKV[k].Value)
		}
		output := reducef(intermediateKV[i].Key, values)

		// this is the correct format for each line of Reduce output.
		if _, err := fmt.Fprintf(tempFile, "%v %v\n", intermediateKV[i].Key, output); err != nil {
			return err
		}

		i = j
	}

	// commit tempFile
	_ = tempFile.Close()
	currentDir, _ := filepath.Abs("./")
	outputFilePath := filepath.Join(currentDir, outputFileName)
	if os.Rename(tempFile.Name(), outputFilePath) != nil {
		return err
	}
	w.outputFile = outputFilePath

	w.lock.Lock()
	w.status = WorkerStatus_workedWorker
	w.lock.Unlock()
	log.Printf("Worker:[%s] handleReduceTask done", w)
	return nil
}

func (w *Worker) outputFileName(numOfReduceTask string) string {
	return fmt.Sprintf("mr-out-%s", numOfReduceTask)
}

func (w *Worker) replyReduceTask() error {
	// already replied
	if w.status >= WorkerStatus_repliedWorker {
		return nil
	}

	args := ReduceTaskArgs{
		Id:         w.id,
		OutputFile: w.outputFile,
	}
	reply := ReduceTaskReply{}
	if err := w.ReduceTask(&args, &reply); err != nil {
		log.Printf("Worker:[%s] replyReduceTask err:[%v]", w, err)
		return err
	}
	if reply.Err != "" {
		log.Printf("Worker:[%s] replyMapTask err:[%v]", w, reply.Err)
		// ??????worker??????????????????????????????reply??????coord, ??????coord reply???err???worker?????????????????????????????????????????????
	}

	w.lock.Lock()
	w.status = WorkerStatus_repliedWorker
	w.lock.Unlock()
	log.Printf("Worker:[%s] replyReduceTask done", w)
	return nil
}

func (w *Worker) reset() {
	w.id = newId()
	w.taskType = 0
	w.status = WorkerStatus_idleWorker
	w.nReduce = 0
	w.inputFile = ""
	w.intermediateFilePathList = []string{}
	w.numOfReduceTask = ""
	w.outputFile = ""
	w.errRetryCount = 0
	w.tmpFileMap = make(map[int]*os.File)
}

func newWorker() *Worker {
	return &Worker{
		instance:                 newId(),
		id:                       newId(),
		taskType:                 0,
		status:                   WorkerStatus_idleWorker,
		nReduce:                  0,
		inputFile:                "",
		intermediateFilePathList: []string{},
		outputFile:               "",
		errRetryCount:            0,
		tmpFileMap:               make(map[int]*os.File),
	}
}

func newId() string {
	rand.Seed(time.Now().UnixNano())
	return strconv.Itoa(rand.Intn(100))
}

func (w *Worker) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	conn, err := grpc.Dial(fmt.Sprintf(":%d", PORT), grpc.WithInsecure())
	if err != nil {
		//log.Printf("Worker:[%v] dialing err:[%v]", w, err)
		return err
	}
	defer conn.Close()
	client := NewMapReduceClient(conn)
	task, err := client.AskTask(context.Background(), args)
	if err != nil {
		//log.Printf("Worker:[%v], call err:[%v]", w, err)
		return err
	}

	// TODO
	reply.Err = task.Err
	reply.NReduce = task.NReduce
	reply.TaskType = task.TaskType
	reply.InputFile = task.InputFile
	reply.NumOfMapTask = task.NumOfMapTask
	reply.IntermediateFilePathList = task.IntermediateFilePathList
	reply.NumOfReduceTask = task.NumOfReduceTask

	return nil
}

func (w *Worker) MapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	conn, err := grpc.Dial(fmt.Sprintf(":%d", PORT), grpc.WithInsecure())
	if err != nil {
		//log.Printf("Worker:[%v] dialing err:[%v]", w, err)
		return err
	}
	defer conn.Close()
	client := NewMapReduceClient(conn)
	task, err := client.MapTask(context.Background(), args)
	if err != nil {
		//log.Printf("Worker:[%v], call err:[%v]", w, err)
		return err
	}

	// TODO
	reply.Err = task.Err
	return nil
}

func (w *Worker) ReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	conn, err := grpc.Dial(fmt.Sprintf(":%d", PORT), grpc.WithInsecure())
	if err != nil {
		//log.Printf("Worker:[%v] dialing err:[%v]", w, err)
		return err
	}
	defer conn.Close()
	client := NewMapReduceClient(conn)
	task, err := client.ReduceTask(context.Background(), args)
	if err != nil {
		//log.Printf("Worker:[%v], call err:[%v]", w, err)
		return err
	}

	// TODO
	reply.Err = task.Err
	return nil
}

func (w *Worker) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	conn, err := grpc.Dial(fmt.Sprintf(":%d", PORT), grpc.WithInsecure())
	if err != nil {
		//log.Printf("Worker:[%v] dialing err:[%v]", w, err)
		return err
	}
	defer conn.Close()
	client := NewMapReduceClient(conn)
	heartbeatReply, err := client.Heartbeat(context.Background(), args)
	if err != nil {
		//log.Printf("Worker:[%v], call err:[%v]", w, err)
		return err
	}

	// TODO
	reply = heartbeatReply
	return nil
}

// heartbeat ???????????????worker???instance???????????????????????? worker?????????task hang??????
// ??????????????????????????????goroutine
// ???????????????coordinator???????????????instance?????????????????????????????????
// ?????? TaskHeartbeatMaxDelayTime <= CoordEvictUnhealthyWorkerTime
// ?????????????????? edge case???worker instance hang????????????maxDelayTime????????????evict???
// ???????????????evict??????????????? ?????? beatsMaxDelay???????????????
func (w *Worker) heartbeat() {
	for {
		w.lock.Lock()
		workerStatus := w.status
		workerId := w.id
		w.lock.Unlock()

		if workerStatus >= WorkerStatus_assignedWorker {
			time.Sleep(TaskHeartbeatInterval)
			continue
		}
		args := HeartbeatArgs{
			Id:  workerId,
			Now: timestamppb.Now(),
		}
		if err := w.Heartbeat(&args, nil); err != nil {
			log.Printf("Worker heartbeat err:[%v]", err)
			w.heartbeatRetryCount++
		}

		// handle error cases
		if w.heartbeatRetryCount >= TaskHeartbeatMaxRetryCount {
			log.Printf("Worker heartbeat retry times had exceed max, will stop worker")
			os.Exit(1)
		}

		time.Sleep(TaskHeartbeatInterval)
	}
}
