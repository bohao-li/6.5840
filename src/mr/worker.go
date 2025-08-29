package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "os"
import "time"
import "sort"
import "path/filepath"


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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := os.Getpid() // simple worker id
	for {
		// 1. request a task
		gtArgs := GetTaskArgs{WorkerID: workerID}
		gtReply := GetTaskReply{}
		if !call("Coordinator.GetTask", &gtArgs, &gtReply) {
			log.Printf("Worker %d: GetTask RPC failed; retrying", workerID)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		task := gtReply.Task
		switch task.TaskType {
		case MapTask:
			if err := doMapTask(&task, mapf); err != nil {
				log.Printf("Worker %d: map task %d failed: %v", workerID, task.TaskID, err)
				// let it timeout & be reassigned
			} else {
				notifyTaskDone(task)
			}
		case ReduceTask:
			if err := doReduceTask(&task, reducef); err != nil {
				log.Printf("Worker %d: reduce task %d failed: %v", workerID, task.TaskID, err)
			} else {
				notifyTaskDone(task)
			}
		case WaitTask:
			time.Sleep(500 * time.Millisecond)
		case DoneTask:
			log.Printf("Worker %d: all work done; exiting", workerID)
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// notifyTaskDone reports task completion to coordinator.
func notifyTaskDone(t Task) {
	args := TaskCompletedArgs{TaskID: t.TaskID, TaskType: t.TaskType}
	reply := TaskCompletedReply{}
	if !call("Coordinator.TaskCompleted", &args, &reply) {
		log.Printf("Failed to notify completion for task %d", t.TaskID)
	}
}

// doMapTask runs the user map function and writes intermediate files.
func doMapTask(t *Task, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(t.InputFile)
	if err != nil {
		return err
	}
	kva := mapf(t.InputFile, string(content))

	// ensure intermediate directory exists
	intermediateDir := "mr-tmp"
	if err := os.MkdirAll(intermediateDir, 0755); err != nil {
		return err
	}

	// create one file per reduce partition
	encoders := make([]*json.Encoder, t.NReduce)
	files := make([]*os.File, t.NReduce)
	for r := 0; r < t.NReduce; r++ {
		fname := fmt.Sprintf("mr-%d-%d", t.TaskID, r)
		f, err := os.CreateTemp(intermediateDir, fname+"-tmp-*")
		if err != nil { return err }
		encoders[r] = json.NewEncoder(f)
		files[r] = f
	}
	// partition
	for _, kv := range kva {
		r := ihash(kv.Key) % t.NReduce
		if err := encoders[r].Encode(&kv); err != nil {
			return err
		}
	}
	// atomically rename temp files
	for r, f := range files {
		finalName := filepath.Join(intermediateDir, fmt.Sprintf("mr-%d-%d", t.TaskID, r))
		f.Close()
		os.Rename(f.Name(), finalName)
	}
	return nil
}

// doReduceTask aggregates intermediate kvs and writes output.
func doReduceTask(t *Task, reducef func(string, []string) string) error {
	// gather all intermediate files for this reduce id
	pattern := filepath.Join("mr-tmp", fmt.Sprintf("mr-*-%d", t.ReduceID))
	matches, err := filepath.Glob(pattern)
	if err != nil { return err }
	kvMap := make(map[string][]string)
	for _, fname := range matches {
		f, err := os.Open(fname)
		if err != nil { return err }
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err.Error() == "EOF" { break }
				// break on any decode error (EOF is expected). Use os.Is... not needed here.
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
		f.Close()
	}
	// sort keys for deterministic output
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap { keys = append(keys, k) }
	sort.Strings(keys)

	// write output atomically
	tmp, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-tmp-*", t.ReduceID))
	if err != nil { return err }
	for _, k := range keys {
		out := reducef(k, kvMap[k])
		fmt.Fprintf(tmp, "%v %v\n", k, out)
	}
	tmp.Close()
	finalName := fmt.Sprintf("mr-out-%d", t.ReduceID)
	if err := os.Rename(tmp.Name(), finalName); err != nil { return err }

	// cleanup intermediate files for this reduce partition
	for _, fname := range matches {
		_ = os.Remove(fname)
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
