
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
type ServerInfo struct{
  Me string
  PingTime int64 // Millisecond
    
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
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  me := args.Me
  pingTime := time.Now().Unix()
  
//  fmt.Printf("Ping by %s at %v.\n",me, pingTime)
  vs.Servers[me] = &ServerInfo{me,pingTime}
  //args.Viewnum
  reply.View = View{vs.Viewnum, vs.Primary, vs.Backup}
  //fmt.Println("map: ",vs.Servers)
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
  if vs.Primary=="" && vs.Backup=="" && len(vs.Servers)!=0{
    for me, _ := range vs.Servers {
      vs.Primary = me
      vs.Viewnum ++
    }
    return
  }
  // Your code here.
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
