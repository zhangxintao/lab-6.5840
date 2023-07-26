package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)

		if ok {
			if reply.TaskType == Map && reply.MapFile != "" {
				log.Printf("got map task: %+v", reply.TaskId)
				ProcessMapTask(reply, mapf)
			} else if reply.TaskType == Reduce && len(reply.IntermediateFiles) > 0 {
				log.Printf("got reduce task: %+v", reply.TaskId)
				ProcessReduceTask(reply, reducef)
			} else {
				log.Printf("no task is assigned")
				break
			}
		} else {
			log.Printf("failed to ask for task")
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func AskForFinish(taskId int, taskType TaskType, intermediateFiles []string) {
	args := FinishTaskArgs{TaskId: taskId, TaskType: taskType, IntermediateFiles: intermediateFiles}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		log.Printf("failed to finish task")
	}
}

func ProcessMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) {
	filename := task.MapFile
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
	openFiles := make([]*os.File, task.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		if openFiles[bucket] == nil {
			targetFileName := strings.Join([]string{"mr", strconv.Itoa(task.TaskId), strconv.Itoa(bucket)}, "-")
			openFiles[bucket], err = ioutil.TempFile("", targetFileName+"."+"*")
			log.Printf("created temp file for %v, tmp file:%v", targetFileName, openFiles[bucket].Name())
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
	var intermediates []string
	workingPath, _ := os.Getwd()
	for _, file := range openFiles {
		// rename temp file to target file
		if file != nil {
			realName := filepath.Join(workingPath, strings.Split(filepath.Base(file.Name()), ".")[0])
			log.Printf("renaming %v to %v", file.Name(), realName)
			os.Rename(file.Name(), realName)
			intermediates = append(intermediates, filepath.Base(realName))
		}
	}

	AskForFinish(task.TaskId, task.TaskType, intermediates)
}

func ProcessReduceTask(task RequestTaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, intermediateFile := range task.IntermediateFiles {
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
	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	ofile, _ := os.CreateTemp("", oname+"."+"*")

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

	workingPath, _ := os.Getwd()
	realName := filepath.Join(workingPath, strings.Split(filepath.Base(ofile.Name()), ".")[0])
	log.Printf("reduce, rename %+v to %+v", ofile.Name(), realName)
	os.Rename(ofile.Name(), realName)
	//ofile.Close()

	AskForFinish(task.TaskId, task.TaskType, nil)
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
