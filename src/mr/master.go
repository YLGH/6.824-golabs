package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Master struct {
	mapTasks           chan string
	mapTasksTODO       int
	fileToIdx          map[string]int
	numReduce          int
	reduceTasks        chan int
	reduceTasksTODO    int
	workerTimeout      time.Duration
	workerID           int
	outstandingWorkers map[int]bool
	mu                 sync.Mutex
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

func (m *Master) getMapTasksTODO() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mapTasksTODO
}

func (m *Master) getReduceTasksTODO() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reduceTasksTODO
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if m.getMapTasksTODO() > 0 {
		mapTask := <-m.mapTasks
		if m.getMapTasksTODO() == 0 {
			reply.TaskType = "Nan"
			return nil
		}
		reply.TaskType = "map"
		reply.MapReply.File = mapTask
		reply.MapReply.FileID = m.fileToIdx[mapTask]
		reply.MapReply.NumReduce = m.numReduce

		m.mu.Lock()
		reply.WorkerID = m.workerID
		m.workerID += 1
		m.outstandingWorkers[reply.WorkerID] = false
		m.mu.Unlock()

		go func(workerID int, mapTask string) {
			time.Sleep(m.workerTimeout)
			m.mu.Lock()
			defer m.mu.Unlock()
			if val, ok := m.outstandingWorkers[workerID]; !ok || !val {
				fmt.Printf("Detected worker id=%v failed for map task=%v, adding back to channel\n", workerID, mapTask)
				m.mapTasks <- mapTask
			} else {
				m.mapTasksTODO -= 1
				if m.mapTasksTODO == 0 {
					close(m.mapTasks)
				}
			}
			delete(m.outstandingWorkers, workerID)
		}(reply.WorkerID, mapTask)

	} else if m.getReduceTasksTODO() > 0 {
		reduceTask := <-m.reduceTasks
		if m.getReduceTasksTODO() == 0 {
			reply.TaskType = "Nan"
			return nil
		}

		reply.TaskType = "reduce"
		reduceFiles, err := filepath.Glob(fmt.Sprintf("mr-*-%d", reduceTask))
		if err != nil {
			reduceFiles = make([]string, 0)
		}

		reply.ReduceReply.Files = reduceFiles
		reply.ReduceReply.Partition = reduceTask

		m.mu.Lock()
		reply.WorkerID = m.workerID
		m.workerID += 1
		m.outstandingWorkers[reply.WorkerID] = false
		m.mu.Unlock()

		go func(workerID int, reduceTask int) {
			time.Sleep(m.workerTimeout)
			m.mu.Lock()
			defer m.mu.Unlock()
			if val, ok := m.outstandingWorkers[workerID]; !ok || !val {
				fmt.Printf("Detected worker id=%v failed for reduce task=%v, adding back to channel\n", workerID, reduceTask)
				m.reduceTasks <- reduceTask
			} else {
				m.reduceTasksTODO -= 1
				if m.reduceTasksTODO == 0 {
					close(m.reduceTasks)
				}
			}
			delete(m.outstandingWorkers, workerID)
		}(reply.WorkerID, reduceTask)
	} else {
		reply.TaskType = "done"
	}
	return nil
}

func (m *Master) WorkerFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outstandingWorkers[args.WorkerID] = true
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mapTasksTODO == 0 && m.reduceTasksTODO == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTasks:           make(chan string, len(files)),
		mapTasksTODO:       len(files),
		numReduce:          nReduce,
		reduceTasks:        make(chan int, nReduce),
		reduceTasksTODO:    nReduce,
		workerID:           0,
		outstandingWorkers: make(map[int]bool),
		workerTimeout:      10 * time.Second,
		fileToIdx:          make(map[string]int),
	}

	for idx, file := range files {
		m.fileToIdx[file] = idx
		m.mapTasks <- file

	}
	for partition := 0; partition < nReduce; partition++ {
		m.reduceTasks <- partition
	}

	m.server()
	return &m
}
