package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func tempName(fileName string) string {
	return "temp-" + fileName
}

func reduceName(mapTaskNum, reduceTaskName int) string {
	return "mr-im-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(reduceTaskName)
}

func outName(reduceTaskNum int) string {
	return "mr-out-" + strconv.Itoa(reduceTaskNum)
}

type MapTaskWorker struct {
	mapf func(string, string) []KeyValue
}
type ReduceTaskWorker struct {
	reducef func(string, []string) string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	mapTaskWorker := MapTaskWorker{
		mapf: mapf,
	}
	start(&mapTaskWorker)

	reduceTaskWorker := ReduceTaskWorker{
		reducef: reducef,
	}
	start(&reduceTaskWorker)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// the interface of MapTaskWorker
func (w *MapTaskWorker) doTask(task Task) {
	fileName := task.InputFileName
	nReduce := task.NReduce
	mapTaskNum := task.TaskNum

	// excute the map function
	content, _ := os.ReadFile(fileName)
	kvs := w.mapf(fileName, string(content))

	// create middle file
	reduceFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceFileName := reduceName(mapTaskNum, i)
		reduceFile, _ := os.CreateTemp("", tempName(reduceFileName))
		reduceFiles[i] = reduceFile
	}
	// write to the middle file
	for _, kv := range kvs {
		reduceTaskNum := ihash(kv.Key) % nReduce
		reduceFile := reduceFiles[reduceTaskNum]

		enc := json.NewEncoder(reduceFile)
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("Encode error:", err)
		}
	}
	// rename middle files
	for i := 0; i < nReduce; i++ {
		reduceFile := reduceFiles[i]
		reduceFile.Close()
		os.Rename(reduceFile.Name(), reduceName(mapTaskNum, i))
	}
}

func (*MapTaskWorker) getTaskType() string {
	return "map"
}

// the interface of ReduceTaskWorker
func (w *ReduceTaskWorker) doTask(task Task) {
	reduceTaskNum := task.TaskNum
	mapTaskTotal := task.MapTaskTotal

	// read the key-value from the middle file to keyMap
	kvs := make(map[string][]string)
	for i := 0; i < mapTaskTotal; i++ {
		reduceFileName := reduceName(i, reduceTaskNum)
		readReduceFile(reduceFileName, kvs)
	}

	// create output file
	outFileName := outName(reduceTaskNum)
	outFile, _ := os.CreateTemp("", tempName(outFileName))

	// excute the reduce function
	for key, vals := range kvs {
		res := w.reducef(key, vals)
		line := fmt.Sprintf("%v %v", key, res)
		fmt.Fprintln(outFile, line)
	}
	os.Rename(outFile.Name(), outFileName)
	outFile.Close()
}

func (*ReduceTaskWorker) getTaskType() string {
	return "reduce"
}

func readReduceFile(reduceFileName string, keyMap map[string][]string) {
	reduceFile, err := os.OpenFile(reduceFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Open reduceFile error", err)
	}
	dec := json.NewDecoder(reduceFile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		keyMap[kv.Key] = append(keyMap[kv.Key], kv.Value)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
