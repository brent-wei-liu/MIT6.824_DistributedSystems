package mapreduce
import "container/list"
import (
    "fmt"
    "time"
)

const(
    AVAILABLE = iota
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
go func(){
    WorkerNum := 20000
    for i:=0; i<WorkerNum; i++{
        addr := <-mr.registerChannel
        w := new(WorkerInfo)
        w.address = addr
        w.status = AVAILABLE
        mr.Workers[addr] = w
        fmt.Println(addr, mr.Workers)
    }
}()
    i := 0
    for i<mr.nMap{
        var worker *WorkerInfo
        for{
            for _, v := range mr.Workers{
                if v.status == AVAILABLE {
                    worker = v
                    v.status = WORKING
                    break
                }
            }
            if worker == nil{
                fmt.Println("No work available, sleep!")
                time.Sleep(2*time.Second)
            }else{
                break
            }
        }
        DPrintf("DoWork: DoMap[%d] %s\n", i, worker.address)
        args := &DoJobArgs{mr.file, "Map", i, mr.nReduce}
        var reply DoJobReply;
        ok := call(worker.address, "Worker.DoJob", args, &reply)
        if ok == false {
          fmt.Printf("DoWork: RPC %s DoJob error\n", worker.address)
          worker.status = DISCONNECTED
        }else{
          fmt.Printf("DoWork: RPC %s DoJob Finished!\n", worker.address)
          worker.status = AVAILABLE 
          i++
        }
    }
    i = 0
    for i<mr.nReduce{
        var worker *WorkerInfo
        for{
            for _, v := range mr.Workers{
                if v.status == AVAILABLE {
                    worker = v
                    v.status = WORKING
                    break
                }
            }
            if worker == nil{
                fmt.Println("No work available, sleep!")
                time.Sleep(2*time.Second)
            }else{
                break
            }
        }
        DPrintf("DoWork: DoReduce[%d] %s\n", i, worker.address)
        args := &DoJobArgs{mr.file, "Reduce", i, mr.nMap}
        var reply DoJobReply;
        ok := call(worker.address, "Worker.DoJob", args, &reply)
        if ok == false {
          fmt.Printf("DoWork: RPC %s DoJob error\n", worker.address)
          worker.status = DISCONNECTED
        }else{
          fmt.Printf("DoWork: RPC %s DoJob Finished!\n", worker.address)
          worker.status = AVAILABLE 
          i++
        }
    }

    return mr.KillWorkers()
}
