package mr

import (
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	sync.Mutex
	TaskPool  map[int]CoordinatorTask
	NReduce   int
	NMap      int
	TaskQueue chan CoordinatorTask
}

type ExecutionStatus int64

const (
	NotStarted ExecutionStatus = iota
	Started
	Finished
)

type CoordinatorTaskType int64

const (
	MapTask CoordinatorTaskType = iota
	ReduceTask
)

type CoordinatorTask struct {
	id                int
	taskType          TaskType
	status            ExecutionStatus
	timeout           time.Time
	mapSourceFile     string
	intermediateFiles []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if c.Done() {
		reply.TaskType = NoTask
		return nil
	}

	task, ok := <-c.TaskQueue
	c.Lock()
	defer c.Unlock()
	if ok {
		log.Printf("to assign task: %+v", task)
		reply.TaskType = task.taskType
		reply.TaskId = task.id
		reply.MapFile = task.mapSourceFile
		reply.IntermediateFiles = task.intermediateFiles
		reply.NReduce = c.NReduce
		task.status = Started
		task.timeout = time.Now().Add(10 * time.Second)
		if task.taskType == Map {
			c.TaskPool[task.id] = task
		} else {
			c.TaskPool[c.NMap+task.id] = task
		}
		return nil
	} else {
		log.Printf("no more task available in channel")
		reply.TaskType = NoTask
		return nil
	}
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if c.Done() {
		return nil
	}

	c.Lock()
	defer c.Unlock()
	log.Printf("finishing task:%+v, type:%v", args.TaskId, args.TaskType)
	index := args.TaskId
	if args.TaskType == Reduce {
		index += c.NMap
	}

	if c.TaskPool[index].taskType == Map {
		if args.IntermediateFiles == nil || len(args.IntermediateFiles) == 0 {
			return errors.New("no intermediate files")
		}
		for _, intermediate := range args.IntermediateFiles {
			parts := strings.Split(intermediate, "-")
			y, err := strconv.Atoi(parts[2])
			if err != nil {
				log.Println(err)
			}
			mapKey := c.NMap + y
			task, ok := c.TaskPool[mapKey]
			if !ok {
				task = CoordinatorTask{id: y, taskType: Reduce, status: NotStarted, intermediateFiles: []string{}}
			}
			task.intermediateFiles = append(task.intermediateFiles, intermediate)
			c.TaskPool[mapKey] = task
			log.Printf("added reduce task:%+v, mapKey:%+v, source:%+v", y, c.NMap+y, intermediate)
		}
		delete(c.TaskPool, args.TaskId)
		log.Printf("done map: %+v, remaining:%+v", args.TaskId, len(c.TaskPool))
		if c.mapDone() {
			c.prepareReduceTasks()
		}
	} else {
		mapKey := c.NMap + args.TaskId
		delete(c.TaskPool, mapKey)
		log.Printf("done reduce: %+v,  remaining:%+v", args.TaskId, len(c.TaskPool))
	}
	return nil
}

func (c *Coordinator) mapDone() bool {
	for _, task := range c.TaskPool {
		if task.taskType == Map {
			log.Printf("found map hasn't done: +%v, sourceFile:%+v", task.id, task.mapSourceFile)
			return false
		}
	}

	return true
}

func (c *Coordinator) prepareReduceTasks() {
	for _, reduceTask := range c.TaskPool {
		if reduceTask.taskType == Reduce && reduceTask.status == NotStarted {
			c.TaskQueue <- reduceTask
		} else {
			log.Fatalf("found non-reduce task while preparing for reduce")
		}
	}
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
	c.Lock()
	defer c.Unlock()

	log.Printf("check done:%v", len(c.TaskPool))
	return len(c.TaskPool) == 0
}

func (c *Coordinator) checkTimeout() {
	for {
		c.Lock()
		for id, task := range c.TaskPool {
			if task.status == Started {
				log.Printf("checking timeout for task:%+v, timeout at:%+v", task.id, task.timeout)
				if task.timeout.Before(time.Now()) {
					log.Printf("task timeout: %+v", task)
					task.timeout = time.Now().Add(10 * time.Second)
					task.status = NotStarted
					c.TaskPool[id] = task
					c.TaskQueue <- task
				}
			}
		}
		c.Unlock()
		time.Sleep(1 * time.Second)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskPool:  make(map[int]CoordinatorTask),
		TaskQueue: make(chan CoordinatorTask, int(math.Max(float64(len(files)), float64(nReduce)))),
		NReduce:   nReduce,
		NMap:      len(files),
	}
	log.Printf("coordinator inited")

	for i, filename := range files {
		c.TaskPool[i] = CoordinatorTask{id: i, taskType: Map, status: NotStarted, mapSourceFile: filename}
	}
	log.Printf("task pool created")

	for _, task := range c.TaskPool {
		log.Printf("pushing task:%+v", task)
		c.TaskQueue <- task
	}
	log.Printf("task queued")

	go c.checkTimeout()

	c.server()
	path, _ := os.Getwd()
	log.Printf("coordinator started, %+v, current: %+v", os.TempDir(), path)
	return &c
}
