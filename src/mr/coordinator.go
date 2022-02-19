package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files          []string
	mapfinished    int
	reducefinished int
	nmap           int
	nreduce        int
	maptasks       []int //indicate tasks states
	reducetasks    []int //0:allocatable, 1:waiting, 2:finished
	mu             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) MapTaskFinish(args *WorkerArgs, reply *ExampleReply) error {
	c.mu.Lock()
	c.maptasks[args.Taskindex] = 2
	c.mapfinished++
	c.mu.Unlock()
	return nil
}
func (c *Coordinator) ReduceTaskFinish(args *WorkerArgs, reply *ExampleReply) error {
	c.mu.Lock()
	c.reducetasks[args.Taskindex] = 2
	c.reducefinished++
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	if c.mapfinished < c.nmap {
		taskindex := -1
		for i := 0; i < c.nmap; i++ {
			if c.maptasks[i] == 0 {
				taskindex = i
				break
			}
		}
		if taskindex == -1 {
			reply.Tasktype = 2
			c.mu.Unlock()
		} else {
			c.maptasks[taskindex] = 1
			reply.Tasktype = 0
			reply.Filename = c.files[taskindex]
			reply.Taskindex = taskindex
			reply.Nreduce = c.nreduce
			c.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.mu.Lock()
				if c.maptasks[taskindex] != 2 {
					c.maptasks[taskindex] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else if c.mapfinished == c.nmap && c.reducefinished < c.nreduce {
		taskindex := -1
		for i := 0; i < c.nreduce; i++ {
			if c.reducetasks[i] == 0 {
				taskindex = i
				break
			}
		}
		if taskindex == -1 {
			reply.Tasktype = 2
			c.mu.Unlock()
		} else {
			c.reducetasks[taskindex] = 1
			reply.Tasktype = 1
			reply.Taskindex = taskindex
			reply.Nmap = c.nmap
			c.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.mu.Lock()
				if c.reducetasks[taskindex] != 2 {
					c.reducetasks[taskindex] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else {
		reply.Tasktype = 3
		c.mu.Unlock()
	}
	return nil
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
	c.mu.Lock()
	if c.mapfinished == c.nmap && c.reducefinished == c.nreduce {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nmap = len(files)
	c.nreduce = nReduce
	c.maptasks = make([]int, c.nmap)
	c.reducetasks = make([]int, c.nreduce)
	c.mapfinished = 0
	c.reducefinished = 0
	// Your code here.

	c.server()
	return &c
}
