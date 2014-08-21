package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  view viewservice.View
  kv map[string]string
  isPrimary bool
  backup string
  mu sync.Mutex
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  fmt.Println("PBServer.Put: ", args)
  
  if pb.isPrimary {
        if args.DoHash{
            prev_val, exists := pb.kv[args.Key]
            if exists == false {
                prev_val = ""
            }
            args.Value = strconv.Itoa(int(hash(prev_val + args.Value))) // new value is the hash of the prev_val and value
            reply.PreviousValue = prev_val
        }
        pb.kv[args.Key] = args.Value
        reply.Err = OK

 
        fmt.Println(" kv:",pb.kv, " ",pb.me)
        if pb.backup != ""{
            fmt.Printf("Primary:%v forward put to backup:%v\n",pb.me, pb.backup)
            args1 := &ReceivePutArgs{Key:args.Key, Value:args.Value}
            var reply1 ReceivePutReply
            ok := call(pb.backup, "PBServer.ReceivePut", args1, &reply1)
            //if ok == false || reply.knum != len(pb.kv) {
            for ok == false || reply1.Err != OK {
                fmt.Printf("Primary received error from backup when trying send getupdate! OK: %t reply.Err: %s\n", ok, reply1.Err)
                time.Sleep(viewservice.PingInterval)
                pb.tick()
                ok = call(pb.backup, "PBServer.ReceivePut", args1, &reply1)
            }
        }
        reply.Err = OK
  } else if !pb.isPrimary {
      DPrintf("I %s am not primary and cannot put this!", pb.me)
      reply.Err = ErrWrongServer
  }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
//  var ok bool
  fmt.Println("PBServer.Get:",args.Key)
  fmt.Println(" kv:",pb.kv, " ",pb.me)
  
  reply.Value = pb.kv[args.Key]
//  if ok == false {
//    return fmt.Errorf("Get(%v) failed!", args.Key)
//  }
  return nil
}
func (pb *PBServer) ReceivePut(args *ReceivePutArgs, reply *ReceivePutReply) error {
  // Your code here.
    if pb.backup == pb.me{
        fmt.Printf("server %s received getupdate request %v! state is %v\n", pb.me, args.Key, args.Value)
//        pb.SequenceHandled(args.ClientID, args.SeqNum)
//        pb.client_map[args.ClientID][args.SeqNum] = args.Value
        pb.kv[args.Key] = args.Value
        reply.Err = OK
    } else {
        DPrintf("I %s am not backup and cannot get this!", pb.me)
        reply.Err = ErrWrongServer
    }
    return nil

}

func (pb *PBServer) TransferState(args *TransferArgs, reply *TransferReply) error {
  // Your code here.
//  var ok bool
  if pb.isPrimary {
    return fmt.Errorf("Receive Backup database error! I am Primary!");
  }
  pb.kv = args.KV
  reply.knum = len(pb.kv)
  fmt.Println("PBServer.Receive:",pb.kv)
  fmt.Println("reply.knum=", reply.knum)
//  if ok == false {
//    return fmt.Errorf("Get(%v) failed!", args.Key)
//  }
  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
   view, _ := pb.vs.Ping(pb.view.Viewnum)
   if view.Primary == pb.me{
     pb.isPrimary = true
   }else{
     pb.isPrimary = false
   }
   if view.Viewnum != pb.view.Viewnum {
    // transfer all key/value from the current primary to a new backup
     if pb.isPrimary && view.Backup != ""{
        fmt.Println("Transfer database to backup:", view.Backup)
        
        args := &TransferArgs{pb.kv}
        var reply TransferReply
        ok := call(view.Backup, "PBServer.TransferState", args, &reply)
        //if ok == false || reply.knum != len(pb.kv) {
        if ok == false {
           fmt.Printf("Transfer database to backup failed! reply.knum=%v len(pb.kv)=%v\n", reply.knum, len(pb.kv))
           //err := pb.Transfer( view.Backup )
          //if err == nil {
          //}

        }else{
          pb.backup = view.Backup
          pb.view = view
        }


     }else{
        pb.view = view
        pb.backup = view.Backup
     }
   }
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.kv = make(map[string]string)
  pb.isPrimary = false
  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
