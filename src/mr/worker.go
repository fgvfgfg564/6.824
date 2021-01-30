package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "sort"
import "os"
import "io/ioutil"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	args := Args{Task: TASK_INIT, Number: 0}
	reply := Reply{}

	for {
		call("Master.Process", &args, &reply)
		reply.Number--
		if reply.Task == TASK_EXIT {
			return
		}
		if reply.Task == TASK_MAP {
			log.Printf("Receive map task %d", reply.Number)
			mapping(mapf, reply.Number, reply.Filename, reply.NMapReduce)
		}
		if reply.Task == TASK_REDUCE {
			log.Printf("Receive reduce task %d", reply.Number)
			reducing(reducef, reply.Number, reply.NMapReduce)
		}
		args.Task = reply.Task
		args.Number = reply.Number
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func mapping(mapf func(string, string) []KeyValue,
	taskNumber int, filename string, nReduce int) {

	var res []ByKey
	for i := 0; i < nReduce; i++ {
		res = append(res, ByKey{})
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
	temp := mapf(filename, string(content))
	for _, kv := range temp {
		hashnum := ihash(kv.Key) % nReduce
		res[hashnum] = append(res[hashnum], kv)
	}
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", taskNumber, i)
		ofile, _ := os.Create(filename)
		enc := json.NewEncoder(ofile)

		sort.Sort(res[i])
		for _, kv := range res[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Encoding failed %v", filename)
			}
		}
		ofile.Close()
	}
}

func reducing(reducef func(string, []string) string, taskNumber int,
	nMap int) {

	ofilename := fmt.Sprintf("mr-out-%d", taskNumber)
	ofile, _ := os.Create(ofilename)
	kva := ByKey{}

	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(kva)
	i := 0
	for i < len(kva) {
		j := i
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args *Args, reply *Reply) bool {
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
