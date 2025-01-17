package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need to uncomment these:
 import "time"
 import "crypto/rand"
 import "math/big"



type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
  id int64
  seq_num int64
}


func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here
  ck.id = nrand()
  ck.seq_num = 0
  return ck
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}
//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    //fmt.Println("rpc.dial error");
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }
  fmt.Println(err)
  return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

  // Your code here.
  args := &GetArgs{key}
  var reply GetReply
  DPrintf("Client[%v]: send get(%v) request to primary:%v\n",ck.id, key, ck.vs.Primary() ) 
  ok := call(ck.vs.Primary(), "PBServer.Get", args, &reply)
  for ok == false || reply.Err != OK{
      DPrintf("Client[%v]: Get request receive error! ok:%v reply.Err:%v\n",ck.id, ok, reply.Err)
      DPrintf("Client[%v]: re-try sending get(%v) request to primary:%v\n",ck.id, key, ck.vs.Primary() ) 
      ok = call(ck.vs.Primary(), "PBServer.Get", args, &reply)
      time.Sleep(viewservice.PingInterval)
  }
  return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
    // Your code here.
    ck.seq_num++
    fmt.Printf("Clent: sends put request with seq_num:%v\n", ck.seq_num)

    args := &PutArgs{Key: key, Value: value, DoHash: dohash, SeqNum:ck.seq_num, ClientID: ck.id}
    var reply PutReply
    ok := call(ck.vs.Primary(), "PBServer.Put", args, &reply)
    for ok == false || reply.Err != OK  {
        fmt.Printf("Client: received failed put request! OK: %t reply.Err: %s\n", ok, reply.Err)
        fmt.Printf("Client: re-try sends put request with seq_num:%v \n", ck.seq_num)

        ok = call(ck.vs.Primary(), "PBServer.Put", args, &reply)
        time.Sleep(viewservice.PingInterval)
    }
    return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
