package mr

import "log"
import "net"

import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "fmt"

const (
	FREE int8 = iota
	RUNNING
	FINISHED
)

const T_TIMEOUT = 10

type Master struct {
	// Your definitions here.
	mapTaskStatus    []int8
	reduceTaskStatus []int8
	files            []string
	interCount       int
	finalCount       int
	nMap             int
	nReduce          int
	mutex            sync.Mutex
	cond             sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Process(args *Args, reply *Reply) error {

	// Handle finished jobs
	if args.Task == TASK_MAP {
		m.mutex.Lock()
		if m.mapTaskStatus[args.Number] != FINISHED {
			m.interCount++
			if m.interCount == m.nMap {
				m.cond.Broadcast()
			}
			m.mapTaskStatus[args.Number] = FINISHED
		}
		m.mutex.Unlock()
	}
	if args.Task == TASK_REDUCE {
		m.mutex.Lock()
		if m.reduceTaskStatus[args.Number] != FINISHED {
			m.finalCount++
			m.reduceTaskStatus[args.Number] = FINISHED
		}
		m.mutex.Unlock()
		if m.finalCount == m.nReduce {
			reply.Task = TASK_EXIT
			return nil
		}
	}
	for {
		m.mutex.Lock()
		if m.interCount != m.nMap {
			for i := m.nMap - 1; i >= 0; i-- {
				if m.mapTaskStatus[i] == FREE {
					m.mapTaskStatus[i] = RUNNING
					m.mutex.Unlock()

					reply.Task = TASK_MAP
					reply.Number = i + 1
					reply.NMapReduce = m.nReduce
					reply.Filename = m.files[i]

					log.Printf("Distribute map task %d", i)
					go m.Monitor(TASK_MAP, i)
					return nil
				}
			}
		} else {
			for i := m.nReduce - 1; i >= 0; i-- {
				if m.reduceTaskStatus[i] == FREE {
					m.reduceTaskStatus[i] = RUNNING
					m.mutex.Unlock()

					reply.Number = i + 1
					reply.Task = TASK_REDUCE
					reply.NMapReduce = m.nMap

					log.Printf("Distribute reduce task %d", i)
					go m.Monitor(TASK_REDUCE, i)
					return nil
				}
			}
		}

		m.mutex.Unlock()
		m.cond.L.Lock()
		m.cond.Wait()
		m.cond.L.Unlock()
	}
}

func (m *Master) Monitor(task int, taskNumber int) error {
	time.Sleep(T_TIMEOUT * time.Second)
	if task == TASK_MAP && m.mapTaskStatus[taskNumber] == RUNNING {
		m.mapTaskStatus[taskNumber] = FREE
		m.cond.Signal()
	}
	if task == TASK_REDUCE && m.reduceTaskStatus[taskNumber] == RUNNING {
		m.reduceTaskStatus[taskNumber] = FREE
		m.cond.Signal()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.finalCount == m.nReduce

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.nMap = len(files)
	m.nReduce = nReduce
	m.files = files
	copy(m.files, files)
	for _, i := range m.files {
		fmt.Printf("%v\n", i)
	}
	for i := 0; i < len(files); i++ {
		m.mapTaskStatus = append(m.mapTaskStatus, FREE)
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTaskStatus = append(m.reduceTaskStatus, FREE)
	}
	// Your code here.
	m.interCount = 0
	m.finalCount = 0
	m.mutex = sync.Mutex{}
	m.cond = sync.Cond{L: &sync.Mutex{}}

	m.server()
	return &m
}
