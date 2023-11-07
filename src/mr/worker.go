package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		//睡眠一秒再接任务
		time.Sleep(time.Second)
		//初始化一个结构体
		args := ASKArgs{}
		reply := ASKReply{}
		//worker来询问任务
		callAskTask(&args, &reply)
		taskNumber := reply.TaskNumber
		switch reply.State {
		case 0:
			worker_map(mapf, reducef, &reply)
		case 1:
			worker_reduce(mapf, reducef, &reply)
		case 2:
			continue //暂时没有任务，等待下一次申请
		case 3:
			break //所有任务均已完成，worker停止工作
		}
		//当前这个任务结束之后，会给coordinator返回一个任务结束的信号
		Args := FinishAgrs{State: reply.State, TaskNumber: taskNumber}
		Reply := FinishReply{}
		callFinishTask(&Args, &Reply)
		if Reply.State == 1 {
			break
		}
	}
}

func worker_map(mapf func(string, string) []KeyValue, reducef func(string, []string) string, reply *ASKReply) {
	taskNumber := reply.TaskNumber
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open mapTask file", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read mapTask file", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	//写入mr-taskNumber-y文件中
	WriteMiddleFile(kva, taskNumber, reply.NReduce)
}

func worker_reduce(mapf func(string, string) []KeyValue, reducef func(string, []string) string, reply *ASKReply) {
	taskNumber := reply.TaskNumber
	intermediate := []KeyValue{}
	nmap := reply.NMap
	for i := 0; i < nmap; i++ {
		mapFile := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskNumber)
		inputFile, err := os.OpenFile(mapFile, os.O_RDONLY, 0666)
		if err != nil {
			log.Fatalf("can not open reduceTask", mapFile)
		}
		dec := json.NewDecoder(inputFile)
		for {
			var kv []KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv...)
		}
	}
	sort.Sort(ByKey(intermediate))
	outFile := "mr-out-" + strconv.Itoa(taskNumber)
	tempReduceFile, err := ioutil.TempFile("", "mr-reduce-*")
	if err != nil {
		log.Fatalf("cannot open", outFile)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempReduceFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempReduceFile.Close()
	os.Rename(tempReduceFile.Name(), outFile)
}

func WriteMiddleFile(kva []KeyValue, taskNumber int, nReduce int) bool {
	//创建一个二维数组
	buffer := make([][]KeyValue, nReduce)
	for _, value := range kva {
		//将value.key作为参数传入ihash中，将生成的随机数mod nReduce
		//将对应的数据填入到buffer中
		area := (ihash(value.Key)) % nReduce
		buffer[area] = append(buffer[area], value)
	}
	for area, output := range buffer {
		outputFile := "mr-" + strconv.Itoa(taskNumber) + "-" + strconv.Itoa(area)
		//创建一个临时文件，第一个参数表示临时目录，第二个参数表示文件名
		//"mr-map-*"：这是 TempFile 函数的第二个参数，表示临时文件的文件名的前缀。* 是一个通配符
		//表示 TempFile 会在前缀后面自动生成一个唯一的随机字符串来构成临时文件的完整文件名。
		tempMapFile, err := ioutil.TempFile("", "mr-map-*")
		if err != nil {
			log.Fatalf("cannot open tempMapFile")
		}
		//函数调用 json.NewEncoder(tempMapFile) 将创建一个JSON编码器
		//并将它与tempMapFile关联。之后，我们可以使用这个JSON编码器来将数据编码为JSON格式
		//并写入tempMapFile代表的临时文件。
		enc := json.NewEncoder(tempMapFile)
		//将output转换为json格式，output应该是一个keyvalue形式的数组
		//然后将编码之后的东西写入tempMapFile文件中
		err = enc.Encode(output)
		if err != nil {
			return false
		}
		tempMapFile.Close()
		//不太明白为什么这样做
		os.Rename(tempMapFile.Name(), outputFile) //通过原子地重命名避免写入时崩溃，导致内容不完整
	}
	return true
}

func callAskTask(args *ASKArgs, reply *ASKReply) {
	call("Coordinator.ASKTask", &args, &reply)
}

func callFinishTask(args *FinishAgrs, reply *FinishReply) {
	call("Coordinator.FinishTask", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// 调用coordinatorSock函数，该函数返回一个调用者的一个数值id
	// 形式类似于："/var/tmp/824-mr-" + "12138"
	sockname := coordinatorSock()
	//DialHTTP connects to an HTTP RPC server at the specified network address
	//listening on the default HTTP RPC path.
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
