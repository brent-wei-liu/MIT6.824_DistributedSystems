package mapreduce
import "container/list"
import (
    "fmt"
//    "time"
)

const(
    AVALABLE = iota
    WORKING
    DISCONNECTED
)
type WorkerInfo struct {
  address string
  // You can add definitions here.
  status int
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  
//    time.Sleep(2*time.Second)
    addr := <-mr.registerChannel
    w := new(WorkerInfo)
    w.address = addr
    w.status = AVALABLE
    mr.Workers[addr] = w
    fmt.Println(addr, mr.Workers)
   
    for i:=0; i<mr.nMap; i++{
        DPrintf("DoWork: DoMap %s\n", w.address)
        args := &DoJobArgs{mr.file, "Map", i, mr.nReduce}
        var reply DoJobReply;
        ok := call(w.address, "Worker.DoJob", args, &reply)
        if ok == false {
          fmt.Printf("DoWork: RPC %s DoJob error\n", w.address)
        }else{
          fmt.Printf("DoWork: RPC %s DoJob Finished!\n", w.address)
        }
    }

    for i:=0; i<mr.nReduce; i++{
        DPrintf("DoWork: DoReduce %s\n", w.address)
        args := &DoJobArgs{mr.file, "Reduce", i, mr.nMap}
        var reply DoJobReply;
        ok := call(w.address, "Worker.DoJob", args, &reply)
        if ok == false {
          fmt.Printf("DoWork: RPC %s DoJob error\n", w.address)
        }else{
          fmt.Printf("DoWork: RPC %s DoJob Finished!\n", w.address)
        }
    }
    return mr.KillWorkers()
}
