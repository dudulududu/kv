package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"

	kvpb "../proto/kv"
	rfrpc "../rpc/raft"
)

type Clerk struct {
	servers []string
	cid     int64
	seq     int
	leader  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = nrand()
	ck.seq = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := kvpb.CommandArg{Key: key, Value: "", Op: "Get", Cid: ck.cid, Seq: int32(ck.seq + 1)}
	ck.seq++
	// log.Printf("%v", args)
	reply := kvpb.CommandReply{}
	i := 0
	n := len(ck.servers)
	for {
		leader := (ck.leader + i) % n
		err := rfrpc.Call(ck.servers[leader], "Get", &args, &reply, "kv")
		if err != nil || reply.Err == ErrWrongLeader {
			// log.Printf("%v %v", ck.servers[leader], err)
			i++
			continue
		}
		ck.leader = leader
		if reply.Err == ErrDiscard {
			i = 0
			args.Seq = int32(ck.seq + 1)
			ck.seq++
			continue
		}
		return reply.Value
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := kvpb.CommandArg{Key: key, Value: value, Op: op, Cid: ck.cid, Seq: int32(ck.seq + 1)}
	// log.Printf("%v", args)
	reply := kvpb.CommandReply{}
	ck.seq++
	i := 0
	n := len(ck.servers)
	for {

		leader := (ck.leader + i) % n
		// log.Printf("%v", leader)
		err := rfrpc.Call(ck.servers[leader], "PutAppend", &args, &reply, "kv")
		if err != nil || reply.Err == ErrWrongLeader {
			log.Printf("%v %v", ck.servers[leader], err)
			i++
			continue
		}
		ck.leader = leader
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
