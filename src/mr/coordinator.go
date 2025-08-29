package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"


type Coordinator struct {
	MapTasks    []Task
	ReduceTasks []Task
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

// GetTask is an RPC handler for workers to request a task.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	for i := range c.MapTasks {
		if c.MapTasks[i].Status == Idle && c.MapTasks[i].WorkerID == 0 {
			reply.Task = &c.MapTasks[i]
			c.MapTasks[i].Status = InProgress
			c.MapTasks[i].WorkerID = args.WorkerID
			c.MapTasks[i].StartTime = time.Now()
			return nil
		}
	}

	allMapsDone := true
	for i := range c.MapTasks {
		if c.MapTasks[i].Status != Completed {
			allMapsDone = false
			break
		}
	}
	if allMapsDone {
		for i := range c.ReduceTasks {
			if c.ReduceTasks[i].Status == Idle && c.ReduceTasks[i].WorkerID == 0 {
				reply.Task = &c.ReduceTasks[i]
				c.ReduceTasks[i].Status = InProgress
				c.ReduceTasks[i].WorkerID = args.WorkerID
				c.ReduceTasks[i].StartTime = time.Now()
				return nil
			}
		}
	}

	// no task available
	reply.Task = nil
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
	fmt.Println("Coordinator server started")
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	allMapsDone := true
	now := time.Now()
	// check map tasks: timeout recovery and completion
	for i := range c.MapTasks {
		t := &c.MapTasks[i]
		if t.Status == InProgress {
			if now.Sub(t.StartTime) > 10*time.Second {
				// reset timed-out task
				t.Status = Idle
				t.WorkerID = 0
			} else {
				allMapsDone = false
			}
		} else if t.Status == Idle {
			allMapsDone = false
		}
	}

	if !allMapsDone {
		return false
	}

	// check reduce tasks: timeout recovery and completion
	allReducesDone := true
	now = time.Now()
	for i := range c.ReduceTasks {
		t := &c.ReduceTasks[i]
		if t.Status == InProgress {
			if now.Sub(t.StartTime) > 10*time.Second {
				t.Status = Idle
				t.WorkerID = 0
			} else {
				allReducesDone = false
			}
		} else if t.Status == Idle {
			allReducesDone = false
		}
	}

	if allMapsDone && allReducesDone {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:    make([]Task, 0),
		ReduceTasks: make([]Task, 0),
	}

	for _, file := range files {
		task := Task{
			TaskType: MapTask,
			InputFile: file,
			Status:  Idle,
			NReduce: nReduce,
		}
		c.MapTasks = append(c.MapTasks, task)
	}

	// Create reduce tasks
	for i := 0; i < nReduce; i++ {
		task := Task{
			TaskType: ReduceTask,
			ReduceID: i,
			Status:  Idle,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}

	c.server()
	return &c
}
