package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"
import "sync"


type Coordinator struct {
	mu          sync.Mutex
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
	c.mu.Lock()
    defer c.mu.Unlock()

	for i := range c.MapTasks {
		if c.MapTasks[i].Status == Idle && c.MapTasks[i].WorkerID == 0 {
			c.MapTasks[i].Status = InProgress
			c.MapTasks[i].WorkerID = args.WorkerID
			c.MapTasks[i].StartTime = time.Now()

			reply.Task = c.MapTasks[i]

			// print assigned task
			fmt.Printf("Assigned Map Task: File=%s Worker=%d\n",
				c.MapTasks[i].InputFile, c.MapTasks[i].WorkerID)
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
				c.ReduceTasks[i].Status = InProgress
				c.ReduceTasks[i].WorkerID = args.WorkerID
				c.ReduceTasks[i].StartTime = time.Now()

				reply.Task = c.ReduceTasks[i]

				// print assigned task
				fmt.Printf("Assigned Reduce Task: ReduceID=%d Worker=%d\n",
					c.ReduceTasks[i].ReduceID, c.ReduceTasks[i].WorkerID)
				return nil
			}
		}
	}

	allReducesDone := true
	for i := range c.ReduceTasks {
		t := &c.ReduceTasks[i]
		if t.Status != Completed {
			allReducesDone = false
		}
	}

	if allMapsDone && allReducesDone {
		// Everything is done, send a DoneTask
		reply.Task.TaskType = DoneTask
		return nil
	} else {
		// All tasks are in progress, tell the worker to wait
		reply.Task.TaskType = WaitTask
		return nil
	}
}

// TaskCompleted is an RPC handler for workers to report a completed task.
func (c *Coordinator) TaskCompleted(args *TaskCompletedArgs, reply *TaskCompletedReply) error {
    c.mu.Lock() // Use a mutex to protect shared state
    defer c.mu.Unlock()

    if args.TaskType == MapTask {
        // Find the map task by its ID
        for i := range c.MapTasks {
            if c.MapTasks[i].TaskID == args.TaskID {
                c.MapTasks[i].Status = Completed
                // Optional: Store information about intermediate files
                break
            }
        }
    } else if args.TaskType == ReduceTask {
        // Find the reduce task by its ID
        for i := range c.ReduceTasks {
            if c.ReduceTasks[i].TaskID == args.TaskID {
                c.ReduceTasks[i].Status = Completed
                break
            }
        }
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
	

	// Use a counter to generate unique IDs
    taskIDCounter := 0

	for _, file := range files {
		task := Task{
			TaskID:    taskIDCounter,
			TaskType: MapTask,
			InputFile: file,
			Status:  Idle,
			NReduce: nReduce,
		}
		c.MapTasks = append(c.MapTasks, task)
		taskIDCounter++
	}

	// Create reduce tasks
	for i := 0; i < nReduce; i++ {
		task := Task{
			TaskID:    taskIDCounter,
			TaskType: ReduceTask,
			ReduceID: i,
			Status:  Idle,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
		taskIDCounter++
	}

	// Print all tasks for debugging
	fmt.Printf("Coordinator initialized with %d map tasks and %d reduce tasks\n", len(c.MapTasks), len(c.ReduceTasks))
	for i := range c.MapTasks {
		t := c.MapTasks[i]
		fmt.Printf("Map %d: File=%s Status=%v Worker=%d NReduce=%d Start=%v\n",
			i, t.InputFile, t.Status, t.WorkerID, t.NReduce, t.StartTime)
	}
	for i := range c.ReduceTasks {
		t := c.ReduceTasks[i]
		fmt.Printf("Reduce %d: ReduceID=%d Status=%v Worker=%d Start=%v\n",
			i, t.ReduceID, t.Status, t.WorkerID, t.StartTime)
	}

	c.server()
	return &c
}
