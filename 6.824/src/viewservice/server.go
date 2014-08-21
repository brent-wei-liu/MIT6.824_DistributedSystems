
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "errors"
const MAX_VIEWNUM uint = 10
const(
  PRIMARY = iota
  BACKUP
  IDLE
  DEAD
)

type ServerInfo struct{
  Me string
  PingTime time.Time // Millisecond
  Viewnum uint
  IsAlive bool
  Role int
    
}
type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  Servers map[string]*ServerInfo
  Primary string
  Backup  string
  Viewnum uint
  candiPrimary string
  candiBackup  string
  candiViewnum uint
  ACKed  bool //is Current view ACKed? if not VS cannot change 

}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  me := args.Me
  pingTime := time.Now()
  if me=="" {
    err := errors.New("args.Me is nil")
    fmt.Println(err)
    return err
  }
  fmt.Printf("Ping by %s (%v) at %v.\n",me,args.Viewnum, pingTime)
  server, ok := vs.Servers[me] 
  if ok {
    server.PingTime = pingTime  
    server.Viewnum  = args.Viewnum
    //Server restart
    if args.Viewnum == 0 {
      //Primary restart, promote Backup
      if vs.Primary == me {
        fmt.Println("Primary Restarted! Promote Backup!")
        if vs.Backup != "" && vs.ACKed {
          vs.Servers[vs.Backup].Role = PRIMARY
          vs.Servers[me].Role = IDLE
          vs.Primary = vs.Backup
          vs.Backup = ""
          vs.Viewnum ++
          vs.ACKed = false
        }else{
            fmt.Println("P restarted ,system collapse!!!")
            vs.Kill()
            return nil
        }
      //Backup restart
      }else if vs.Backup == me && vs.ACKed {
        vs.Viewnum ++
        vs.ACKed = false
      }
    }

    if server.Role == DEAD {
      server.Role = IDLE
    }
  } else{// new server
    vs.Servers[me] = &ServerInfo{me, pingTime, args.Viewnum, false, IDLE}
  }
 

  //Handle ACKed
  if !vs.ACKed && me == vs.Primary {
    if args.Viewnum == vs.Viewnum {// ACKed , Primary running under current view, VS can change current View
      fmt.Printf("View %v ACKed!\n",vs.Viewnum)
      vs.ACKed = true
    }
  }
 //Very first primary
  if vs.Primary=="" && vs.Backup=="" {
    vs.Primary = me
    vs.Viewnum ++
    vs.ACKed = true
    vs.Servers[me].Viewnum = vs.Viewnum;
    vs.Servers[me].Role = PRIMARY
  }

  //args.Viewnum
  reply.View = View{vs.Viewnum, vs.Primary, vs.Backup}
  fmt.Printf("View(%v , %s, %s) \n",vs.Viewnum, vs.Primary, vs.Backup)
//  fmt.Println("map: ",vs.Servers)
  fmt.Println()
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  reply.View = View{vs.Viewnum, vs.Primary, vs.Backup}
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  var isPDead, isBDead bool
  now := time.Now()
  for _, server := range vs.Servers {
    if now.Sub( server.PingTime) > (PingInterval * DeadPings) {
      if server.Role == PRIMARY {
        isPDead = true
      }
      if server.Role == BACKUP {
        isBDead = true
      }
      server.Role = DEAD
    }
  }
  if vs.ACKed {
    if isPDead {
       fmt.Println("Primary is dead! Promote Backup!");
       if vs.Backup != "" {
          vs.Servers[vs.Backup].Role = PRIMARY
          vs.Primary = vs.Backup
          vs.Backup = ""
          vs.Viewnum ++
          vs.ACKed = false
        }else{
            fmt.Println("P is dead, but no backup, system collapse!!!")
            vs.Kill()
            return 
        }
        return 
    }
    if isBDead {
       fmt.Println("Backup is dead!");
       vs.Servers[vs.Backup].Role = DEAD
       vs.Backup = ""
       vs.Viewnum ++
       vs.ACKed = false
       return
    }
    if vs.Backup == "" {
      for me, server := range vs.Servers{
        if server.Role == IDLE {
          vs.Backup = me
          server.Role = BACKUP
          vs.Viewnum ++
          vs.ACKed = false
          break
        }
      }
    }
    return
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  vs.Primary = ""
  vs.Backup = ""
  // Your vs.* initializations here.

  vs.Servers = make(map[string]*ServerInfo)
  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
