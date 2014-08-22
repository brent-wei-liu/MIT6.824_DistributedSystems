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
const Debug = 1

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
  primary string 
  backup string
  mu sync.Mutex
  client_map map[int64]map[int64]string
}
func (pb *PBServer) SequenceHandled(client_id int64, seq_num int64) bool{
    client, exists := pb.client_map[client_id]
    if exists == false{
        client = make(map[int64]string)
        pb.client_map[client_id] = client
    }
    _, handled := client[seq_num]
    // DPrintf("has sequence been handled? %t", handled)
    return handled
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {    
    pb.mu.Lock()
    defer pb.mu.Unlock()

    DPrintf("Server: %s received put request %v -> %v! DoHash? %t. Seq #: %d\n", pb.me, args.Key, args.Value, args.DoHash, args.SeqNum)
    handled := pb.SequenceHandled(args.ClientID, args.SeqNum)

    if pb.primary == pb.me && ! handled {
        if args.DoHash{
            prev_val, exists := pb.kv[args.Key]
            if exists == false {
                prev_val = ""
            }
            //args.Value = strconv.Itoa(int(hash(prev_val + args.Value))) // new value is the hash of the prev_val and value
            args.Value = strconv.Itoa(int(hash(prev_val , args.Value))) 
            reply.PreviousValue = prev_val
        }
        pb.kv[args.Key] = args.Value
        DPrintf("pb.kv:%v\n\n",pb.kv)
        reply.Err = OK
        pb.client_map[args.ClientID][args.SeqNum] = reply.PreviousValue
        if pb.backup != "" && ! handled{
            pb.PutUpdateRequest(args.Key, args.Value, args.ClientID, args.SeqNum)
        }
    } else if ! (pb.primary == pb.me) {
        DPrintf("Server: I %s am not primary and cannot put this!\n", pb.me)
        reply.Err = ErrWrongServer
    } else {
        DPrintf("Server: sequence %d, client %d has already been handled \n", args.SeqNum, args.ClientID)
        reply.Err = OK
        reply.PreviousValue = pb.client_map[args.ClientID][args.SeqNum]
    }
    return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
//  var ok bool
  DPrintf("PBServer.Get:%v\n",args.Key)
  fmt.Println(" kv:",pb.kv, " ",pb.me)
  
  reply.Value = pb.kv[args.Key]
//  if ok == false {
//    return fmt.Errorf("Get(%v) failed!", args.Key)
//  }
  return nil
}
// PutUpdate RPC handler, backup handles putupdate request from primary
func (pb * PBServer) PutUpdate(args *PutUpdateArgs, reply *PutUpdateReply) error{
    if pb.backup == pb.me{
        DPrintf("Server(Backup): %s received putupdate request %v -> %v! State is %v\n", pb.me, args.Key, args.Value, args.Value)
        pb.kv[args.Key] = args.Value
        pb.SequenceHandled(args.ClientID, args.SeqNum)
        pb.client_map[args.ClientID][args.SeqNum] = args.Value
        reply.Err = OK
    } else {
        DPrintf("Server: I %s am not backup and cannot get this!\n", pb.me)
        reply.Err = ErrWrongServer
    }
    return nil
}
// PutUpdateRequest function- primary calls to send RPC call to backup
func (pb *PBServer) PutUpdateRequest(key string, value string, client_id int64, seq_num int64) {
    DPrintf("Server(Primary): %s sending putupdate request %v -> %v!\n", pb.me, key, value)
    args := &PutUpdateArgs{Key: key, Value: value, ClientID: client_id, SeqNum: seq_num}
    var reply PutUpdateReply
    ok := call(pb.backup, "PBServer.PutUpdate", args, &reply)
    for ok == false || reply.Err != OK{
        DPrintf("Server(Primary): received error from backup %s when trying send putupdate! OK: %t reply.Err: %s\n", pb.backup, ok, reply.Err)
        time.Sleep(viewservice.PingInterval)
        pb.tick()
        DPrintf("Server(Primary): %s re-try sending putupdate request %v -> %v!\n", pb.me, key, value)
        ok = call(pb.backup, "PBServer.PutUpdate", args, &reply)

    }
    return
}

func (pb *PBServer) TransferState(args *TransferArgs, reply *TransferReply) error {
  // Your code here.
//  var ok bool
  if pb.primary == pb.me {
    return fmt.Errorf("Receive Backup database error! I am Primary!\n");
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
   pb.primary = view.Primary
   if view.Viewnum != pb.view.Viewnum {
       // transfer all key/value from the current primary to a new backup
       if pb.primary == pb.me && view.Backup != ""{
          fmt.Println("Transfer database to backup:", view.Backup)
        
          args := &TransferArgs{pb.kv}
          var reply TransferReply
          ok := call(view.Backup, "PBServer.TransferState", args, &reply)
          //if ok == false || reply.knum != len(pb.kv) {
          if ok == false {
              fmt.Printf("Transfer database to backup failed! reply.knum=%v len(pb.kv)=%v\n", reply.knum, len(pb.kv))
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
  pb.client_map = make(map[int64]map[int64]string)
  pb.primary = ""
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
