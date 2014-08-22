package pbservice

//import "hash/fnv"
import ( 
    "fmt"
    "strconv"
    "os"
)
const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.

  // Field names must start with capital letters,
  // otherwise RPC will break.
  SeqNum int64
  ClientID int64
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
}

type GetReply struct {
  Err Err
  Value string
}


// Your RPC definitions here.
type TransferArgs struct{
  KV map[string]string
}
type TransferReply struct{
  knum int
  Err Err
}

type PutUpdateArgs struct{
  Key string
  Value string
  SeqNum int64
  ClientID int64
}
type PutUpdateReply struct{
  Err Err
}

func hash(a string,b string) uint32 {
//   h := fnv.New32a()
//   h.Write([]byte(s))
//   return h.Sum32()
   ai, err := strconv.Atoi(a)
       if err != nil {
           // handle error
           if a == "" {
              ai = 0
           }else{

               fmt.Println(err)
               os.Exit(2)
           }
       }
   

   bi, err := strconv.Atoi(b)
       if err != nil {
           // handle error
           if b == "" {
               bi = 0
           }else{
           fmt.Println(err)
           os.Exit(2)
           }
       }
   
   return uint32(ai + bi)

}

