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
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Task struct {
	taskType   TaskType
	mapTask    MapTask
	reduceTask ReduceTask
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		task, ok := AskForTask()
		if ok {
			if task.taskType == Map {
				log.Printf("got map task: %+v", task.mapTask.Number)
				ProcessMapTask(task, mapf)
			} else {
				log.Printf("got reduce task: %+v", task.reduceTask.Number)
				ProcessReduceTask(task, reducef)
			}
		} else {
			log.Printf("failed to ask for task")
		}
		time.Sleep(2 * time.Second)
	}
}

func AskForTask() (Task, bool) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return Task{taskType: reply.TaskType, mapTask: reply.MapTask, reduceTask: reply.ReduceTask}, true
	} else {
		log.Printf("failed to ask for task")
		return Task{}, false
	}
}

func AskForFinish(taskType TaskType, mapTask FinishMapTaskArgs, reducueTask FinishReduceTaskArgs) {
	args := FinishTaskArgs{TaskType: taskType, MapTask: mapTask, ReduceTask: reducueTask}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		log.Printf("failed to ask for finish task")
	}
}

func ProcessMapTask(task Task, mapf func(string, string) []KeyValue) {
	filename := task.mapTask.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	log.Printf("kv: %+v", kva)
	openFiles := make([]*os.File, task.mapTask.BucketNo)
	var intermediates []string
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.mapTask.BucketNo
		if openFiles[bucket] == nil {
			targetFileName := strings.Join([]string{"mr", strconv.Itoa(task.mapTask.Number), strconv.Itoa(bucket)}, "-")
			log.Printf("creating file %v", targetFileName)
			openFiles[bucket], err = os.Create(targetFileName)
			intermediates = append(intermediates, targetFileName)
			defer openFiles[bucket].Close()
			if err != nil {
				log.Fatalf("cannot create %v", targetFileName)
			}
		}
		enc := json.NewEncoder(openFiles[bucket])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write %v", kv)
		}
	}

	AskForFinish(Map, FinishMapTaskArgs{SourceFilename: filename, IntermediateFiles: intermediates}, FinishReduceTaskArgs{})
}

func ProcessReduceTask(task Task, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, intermediateFile := range task.reduceTask.IntermediateFiles {
		file, err := os.Open(intermediateFile)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFile)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(task.reduceTask.Number)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	AskForFinish(Reduce, FinishMapTaskArgs{}, FinishReduceTaskArgs{Number: task.reduceTask.Number})
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
