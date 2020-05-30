package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

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

	// Your worker implementation here.
	for {
		reply, rc := CallGetTask()
		if !rc {
			return
		}
		fmt.Printf("Worker received reply=%v\n", reply)
		workerID := reply.WorkerID
		if reply.TaskType == "Nan" {
			continue
		} else if reply.TaskType == "done" {
			return
		} else if reply.TaskType == "map" {
			filename := reply.MapReply.File
			fileID := reply.MapReply.FileID
			numReduce := reply.MapReply.NumReduce
			partitionToKVList := make(map[int][]KeyValue)
			for partition := 0; partition < numReduce; partition++ {
				partitionToKVList[partition] = make([]KeyValue, 0)
			}

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

			for _, kv := range kva {
				partition := ihash(kv.Key) % numReduce
				partitionToKVList[partition] = append(partitionToKVList[partition], kv)
			}

			for partition, KVList := range partitionToKVList {
				tmpFile, err := ioutil.TempFile(".", "")
				if err != nil {
					log.Fatalf("Could not create a tmp file for patition=%d", partition)
				}
				enc := json.NewEncoder(tmpFile)

				for _, kv := range KVList {
					enc.Encode(&kv)
				}

				fileName := fmt.Sprintf("mr-%d-%d", fileID, partition)
				tmpFileName := tmpFile.Name()
				err = os.Rename(tmpFileName, fileName)
				if err != nil {
					log.Printf("Could not rename tmpFile=%s to %s\n", tmpFileName, fileName)
				}
			}
		} else if reply.TaskType == "reduce" {
			reduceFiles := reply.ReduceReply.Files
			partition := reply.ReduceReply.Partition
			keysToInstances := make(map[string][]string)
			for _, fileName := range reduceFiles {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("Could not open filename=%s", fileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					keysToInstances[kv.Key] = append(keysToInstances[kv.Key], kv.Value)
				}
			}
			tmpFile, err := ioutil.TempFile(".", "")
			if err != nil {
				log.Fatalf("Could not create a tmp file for reduce worker with partition=%d", partition)
			}
			for key, valueList := range keysToInstances {
				reduceValue := reducef(key, valueList)
				tmpFile.Write([]byte(fmt.Sprintf("%s %s\n", key, reduceValue)))
			}

			fileName := fmt.Sprintf("mr-out-%d", partition)
			tmpFileName := tmpFile.Name()
			err = os.Rename(tmpFileName, fileName)
			if err != nil {
				log.Printf("Could not rename tmpFile=%s to %s\n", tmpFileName, fileName)
			}
		}
		CallWorkerFinished(workerID)
	}
}

func CallGetTask() (GetTaskReply, bool) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	rc := call("Master.GetTask", &args, &reply)
	return reply, rc
}

func CallWorkerFinished(workerID int) (TaskFinishedReply, bool) {
	args := TaskFinishedArgs{WorkerID: workerID}
	reply := TaskFinishedReply{}
	rc := call("Master.WorkerFinished", &args, &reply)
	return reply, rc
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
