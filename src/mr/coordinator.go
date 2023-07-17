package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type taskOpResult struct {
	workerId        int
	taskType        TaskType
	mapTaskParam    MapTask
	reduceTaskParam ReduceTask
	assigned        bool
}

type taskOp struct {
	result chan taskOpResult
}

type taskFinishOp struct {
	result chan bool
	input  FinishTaskArgs
}

type taskFinishOpResult struct {
	taskType        TaskType
	mapTaskParam    FinishMapTaskArgs
	reduceTaskParam FinishReduceTaskArgs
}

var taskRequets chan *taskOp

var finishTaskRequests chan *taskFinishOp

var testChan chan bool

type Coordinator struct {
	// Your definitions here.
	MapTasks map[string]MapExecution
	//ReduceTasks map[string]ReduceExecution
	ReduceTasks []ReduceExecution
	NReduce     int
}

type ExecutionStatus int64

const (
	NotStarted ExecutionStatus = iota
	Started
	Finished
)

type MapExecution struct {
	worker         int
	status         ExecutionStatus
	startedAt      time.Time
	historyWorkers []int
	result         []KeyValue
	intermediates  []string
	expireTime     time.Time
}

type ReduceExecution struct {
	worker            int
	intermediateFiles []string
	status            ExecutionStatus
	startedAt         time.Time
	historyWorkers    []int
	expireTime        time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if !c.Done() {
		var op = taskOp{result: make(chan taskOpResult)}
		taskRequets <- &op
		var result = <-op.result
		if !result.assigned {
			return errors.New("task pending")
		}
		reply.TaskType = result.taskType
		reply.MapTask = result.mapTaskParam
		reply.ReduceTask = result.reduceTaskParam

		return nil
	}

	return errors.New("No more task")
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if !c.Done() {
		var finishTaskop = taskFinishOp{result: make(chan bool), input: *args}
		finishTaskRequests <- &finishTaskop
		var result = <-finishTaskop.result
		if result {
			log.Printf("Successfully finished task: %+v", args)
			return nil
		}
	}
	return errors.New("finish error")
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
	return c.mapDone() && c.reduceDone()
}

func (c *Coordinator) mapDone() bool {
	if len(c.MapTasks) == 0 {
		return false
	}

	for _, mapTask := range c.MapTasks {
		if mapTask.status != Finished {
			return false
		}
	}
	return true
}

func (c *Coordinator) reduceDone() bool {
	if len(c.ReduceTasks) == 0 {
		return false
	}

	for _, reduceTask := range c.ReduceTasks {
		if reduceTask.status != Finished {
			return false
		}
	}
	return true
}

func (c *Coordinator) assignTask() {
	for {
		if c.Done() {
			break
		}
		select {
		case request := <-taskRequets:
			mapTask, workerId := c.tryAssignMap()
			if mapTask != "" {
				request.result <- taskOpResult{assigned: true, taskType: Map, mapTaskParam: MapTask{Filename: mapTask, BucketNo: c.NReduce, Number: workerId}}
				continue
			}

			reduceTask, intermediateFiles := c.tryAssignReduce()
			if reduceTask != -1 {
				log.Printf("assign reduce task: %+v, files:%+v", reduceTask, intermediateFiles)
				request.result <- taskOpResult{assigned: true, taskType: Reduce, reduceTaskParam: ReduceTask{IntermediateFiles: intermediateFiles, Number: reduceTask}}
				continue
			}
			request.result <- taskOpResult{assigned: false}
		}
	}
}

func (c *Coordinator) finishTask() {
	for {
		select {
		case request := <-finishTaskRequests:
			if request.input.TaskType == Map {
				task := c.MapTasks[request.input.MapTask.SourceFilename]
				task.intermediates = request.input.MapTask.IntermediateFiles
				task.status = Finished
				c.MapTasks[request.input.MapTask.SourceFilename] = task
				request.result <- true

				if c.mapDone() {
					// sort all intermediates in MapTasks
					for _, task := range c.MapTasks {
						for _, intermediate := range task.intermediates {
							parts := strings.Split(intermediate, "-")
							y, err := strconv.Atoi(parts[2])
							if err != nil {
								log.Println(err)
							}
							c.ReduceTasks[y].intermediateFiles = append(c.ReduceTasks[y].intermediateFiles, intermediate)
						}
					}
				}
			}

			if request.input.TaskType == Reduce {
				task := c.ReduceTasks[request.input.ReduceTask.Number]
				task.status = Finished
				c.ReduceTasks[request.input.ReduceTask.Number] = task
				request.result <- true
			}
			continue
		}
	}
}

func (c *Coordinator) tryAssignMap() (string, int) {
	for filename, mapTask := range c.MapTasks {
		if mapTask.status == NotStarted {
			mapTask.status = Started
			mapTask.startedAt = time.Now()
			mapTask.historyWorkers = append(mapTask.historyWorkers, mapTask.worker)
			mapTask.result = []KeyValue{}
			mapTask.expireTime = time.Now().Add(10 * time.Second)
			c.MapTasks[filename] = mapTask
			return filename, mapTask.worker
		} else if mapTask.status == Started && time.Now().After(mapTask.expireTime) {
			log.Printf("map task expired: %+v", filename)
			mapTask.expireTime = time.Now().Add(10 * time.Second)
			c.MapTasks[filename] = mapTask
			return filename, mapTask.worker
		}
	}
	return "", -1
}

func (c *Coordinator) tryAssignReduce() (int, []string) {
	for i, reduceTask := range c.ReduceTasks {
		if reduceTask.status == NotStarted && reduceTask.intermediateFiles != nil && len(reduceTask.intermediateFiles) > 0 {
			reduceTask.status = Started
			reduceTask.startedAt = time.Now()
			reduceTask.expireTime = time.Now().Add(10 * time.Second)
			c.ReduceTasks[i] = reduceTask
			return i, reduceTask.intermediateFiles
		} else if reduceTask.status == Started && time.Now().After(reduceTask.expireTime) {
			log.Printf("reduce task expired: %+v", i)
			reduceTask.expireTime = time.Now().Add(10 * time.Second)
			c.ReduceTasks[i] = reduceTask
			return i, reduceTask.intermediateFiles
		}
	}
	return -1, []string{}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:    make(map[string]MapExecution),
		ReduceTasks: make([]ReduceExecution, nReduce),
		NReduce:     nReduce,
	}

	for i, filename := range files {
		c.MapTasks[filename] = MapExecution{
			status:         NotStarted,
			historyWorkers: []int{},
			worker:         i,
		}
	}
	taskRequets = make(chan *taskOp)
	finishTaskRequests = make(chan *taskFinishOp)

	go c.assignTask()
	go c.finishTask()

	c.server()
	return &c
}
