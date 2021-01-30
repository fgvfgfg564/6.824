package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "fmt"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	TASK_INIT = iota
	TASK_REDUCE
	TASK_MAP
	TASK_EXIT
)

type Args struct {
	Task   int
	Number int
}

type Reply struct {
	Task       int
	Number     int
	NMapReduce int
	Filename   string
}

func (a *Reply) print() {
	fmt.Printf("%d %d %d %v\n", a.Task, a.Number, a.NMapReduce, a.Filename)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
