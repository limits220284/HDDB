package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	COORDINATOR_MAP    = 0
	COORDINATOR_REDUCE = 1
	COORDINATOR_FINISH = 2
)
const (
	MAP_PENDING = 0
	MAP_DOING   = 1
	MAP_FINISH  = 2
)
const (
	REDUCE_PENDING = 0
	REDUCE_DOING   = 1
	REDUCE_FINISH  = 2
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//缺少检测故障，不能主动分配任务
//由设备主动申请任务，不需要轮训检查设备是否响应，因此不需要机器号

type Coordinator struct {
	State      int                 //0-map 1-reduce 2-finish
	NMap       int                 //map任务总数
	NReduce    int                 //reduce分区数
	MapTask    map[int]*mapTask    //map任务数组
	ReduceTask map[int]*reduceTask //reduce任务数组
	Mu         sync.Mutex
}

type mapTask struct {
	FileName string
	State    int //0-待做 1-进行中 2-已完成
	RunTime  int
}

type reduceTask struct {
	State   int //0-待做 1-进行中 2-已完成
	RunTime int
}

func (c *Coordinator) TickTick() {
	if c.State == 0 {
		for TaskNumber, task := range c.MapTask {
			if task.State == 1 {
				c.MapTask[TaskNumber].RunTime += 1
				if c.MapTask[TaskNumber].RunTime >= 10 {
					c.MapTask[TaskNumber].State = 0
				}
			}
		}
	} else if c.State == 1 {
		for TaskNumber, task := range c.ReduceTask {
			if task.State == 1 {
				c.ReduceTask[TaskNumber].RunTime += 1
				if c.ReduceTask[TaskNumber].RunTime >= 10 {
					c.ReduceTask[TaskNumber].State = 0
				}
			}
		}
	}
}

// 用来处理来自worker的请求，一般这种请求都需要来一把大锁保平安
// 防止对coordinator中的数据产生影响
func (c *Coordinator) ASKTask(args *ASKArgs, reply *ASKReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	//先把回应设置成2，代表等待状态
	reply.State = 2
	reply.NMap = c.NMap
	reply.NReduce = c.NReduce
	//判断调度器目前的状态，如果是0，表示map阶段，1代表reduce阶段，2代表结束阶段
	switch c.State {
	case 0:
		//如果当前处于map状态，则查询maptask中是否有任务，如果找到了直接reply即可，然后将当前任务状态设置为1
		//表示已经被处理了
		for TaskNumber, task := range c.MapTask {
			if task.State == 0 {
				reply.FileName = task.FileName
				reply.State = 0
				reply.TaskNumber = TaskNumber
				c.MapTask[TaskNumber].State = 1 //表示正在做
				break
			}
		}
	case 1:
		for TaskNumber, task := range c.ReduceTask {
			if task.State == 0 {
				reply.State = 1                    //处于reduce的状态
				reply.TaskNumber = TaskNumber      //任务编号
				c.ReduceTask[TaskNumber].State = 1 //处于正在处理的状态
				break
			}
		}
	case 2:
		reply.State = 3 //处于已经完成的状态
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishAgrs, reply *FinishReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	reply.State = 0
	if args.State == 0 {
		c.MapTask[args.TaskNumber].State = 2
		c.CheckState()
	} else {
		c.ReduceTask[args.TaskNumber].State = 2
		c.CheckState()
		if c.State == 2 {
			reply.State = 1
		}
	}
	return nil
}

func (c *Coordinator) CheckState() {
	for _, task := range c.MapTask {
		if task.State == 0 || task.State == 1 {
			c.State = 0
			return
		}
	}
	for _, task := range c.ReduceTask {
		if task.State == 0 || task.State == 1 {
			c.State = 1
			return
		}
	}
	c.State = 2
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	//这里删除sockname是为了防止该套接字地址已经被占用，出现无法通过该套接字访问的情况

	os.Remove(sockname)
	l, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	ret := false
	c.TickTick() //在每次检查是否完成时，增加任务时间
	if c.State == 2 {
		ret = true
	} else {
		ret = false
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	maptask := make(map[int]*mapTask)
	reducetask := make(map[int]*reduceTask)
	for i, filename := range files {
		maptask[i] = &mapTask{FileName: filename, State: 0, RunTime: 0}
	}
	for j := 0; j < nReduce; j++ {
		reducetask[j] = &reduceTask{State: 0, RunTime: 0}
	}
	c := Coordinator{State: 0, NMap: len(files), NReduce: nReduce, MapTask: maptask, ReduceTask: reducetask, Mu: sync.Mutex{}}
	c.server()
	return &c
}
