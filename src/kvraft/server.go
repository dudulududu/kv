package kvraft

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	kvpb "../proto/kv"
	pb "../proto/raft"
	"../raft"
	"google.golang.org/grpc"
)

const (
	Debug          = 1
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDiscard     = "ErrDiscard"
	ErrTimeOut     = "ErrTimeOut"
)

type Msg struct {
	Cmd *pb.Command
	YN  bool
}

type KVServer struct {
	mu      sync.Mutex
	me      string
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int

	state            map[string]string
	lastApplyIndex   map[int64]int
	lastIncludeIndex int
	commitCh         map[int]chan Msg
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("=="+format, a...)
	}
	return
}

func (kv *KVServer) Get(ctx context.Context, args *kvpb.CommandArg) (*kvpb.CommandReply, error) {
	reply := kvpb.CommandReply{}
	cmd := pb.Command{Key: args.Key, Value: "", Type: "Get", Cid: args.Cid, Seq: args.Seq}
	index, _, isLeader := kv.rf.Start(cmd)
	DPrintf("%v 收到命令 %v, index: %v", kv.me, cmd, index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return &reply, nil
	}
	DPrintf("server %v send Get Cmd, key: %v, index: %v, cid: %v, seq: %v", kv.me, cmd.Key, index, cmd.Cid, cmd.Seq)
	c := make(chan Msg, 2)
	kv.mu.Lock()
	kv.commitCh[index] = c
	kv.mu.Unlock()
	timer := time.NewTimer(time.Duration(1000) * time.Millisecond)
	ok := true
	var msg Msg
	select {
	case msg = <-c:
	case <-timer.C:
		ok = false
	}

	kv.mu.Lock()
	delete(kv.commitCh, index)
	kv.mu.Unlock()
	if !ok {
		DPrintf("%v get cmd timeout, key: %v", index, cmd.Key)
		reply.Err = ErrWrongLeader
		return &reply, nil
	}
	if !msg.YN {
		reply.Err = ErrDiscard
		return &reply, nil
	}
	if cmd.Cid == msg.Cmd.Cid && cmd.Seq == msg.Cmd.Seq && cmd.Key == msg.Cmd.Key && cmd.Value == msg.Cmd.Value && cmd.Type == msg.Cmd.Type {
		kv.mu.Lock()
		DPrintf("%v get cmd ok", index)
		value, f := kv.state[cmd.Key]
		if !f {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = value
		}

		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("%v get cmd key: %v, value: %v, Err: %v", index, cmd.Key, reply.Value, reply.Err)
	return &reply, nil
}

func (kv *KVServer) PutAppend(ctx context.Context, args *kvpb.CommandArg) (*kvpb.CommandReply, error) {
	reply := kvpb.CommandReply{}
	cmd := pb.Command{Key: args.Key, Value: args.Value, Type: args.Op, Cid: args.Cid, Seq: args.Seq}
	index, _, isLeader := kv.rf.Start(cmd)
	DPrintf("%v 收到命令 %v, index: %v", kv.me, cmd, index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return &reply, nil
	}
	DPrintf("server %v send %v Cmd, key: %v, value: %v, index: %v, cid: %v, seq: %v", kv.me, cmd.Type, cmd.Key, cmd.Value, index, cmd.Cid, cmd.Seq)
	c := make(chan Msg, 2)

	kv.mu.Lock()
	kv.commitCh[index] = c
	kv.mu.Unlock()
	timer := time.NewTimer(time.Duration(1000) * time.Millisecond)
	ok := true
	var msg Msg
	select {
	case msg = <-c:
	case <-timer.C:
		ok = false
	}

	kv.mu.Lock()
	delete(kv.commitCh, index)
	kv.mu.Unlock()

	if !ok {
		DPrintf("%v %v cmd timeout, key: %v", index, cmd.Type, cmd.Key)
		reply.Err = ErrWrongLeader
		return &reply, nil
	}
	if !msg.YN {
		reply.Err = ErrDiscard
		return &reply, nil
	}
	if cmd.Cid == msg.Cmd.Cid && cmd.Seq == msg.Cmd.Seq && cmd.Key == msg.Cmd.Key && cmd.Value == msg.Cmd.Value && cmd.Type == msg.Cmd.Type {
		DPrintf("%v %v cmd ok", index, cmd.Type)
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}

	return &reply, nil
}

func (kv *KVServer) readCommit() {
	f := 0
	for {

		DPrintf("server %v , %v", kv.me, f)
		f++
		applyMsg := <-kv.applyCh

		DPrintf("server %v, %v", kv.me, applyMsg)
		if !applyMsg.CommandValid {
			kv.mu.Lock()
			DPrintf("server %v 更新快照", kv.me)
			if kv.lastIncludeIndex < applyMsg.CommandIndex {
				// kv.readPersist(kv.persister.ReadSnapshot())
				kv.lastIncludeIndex = applyMsg.CommandIndex
			}
			DPrintf("server %v 更新完成", kv.me)
			kv.mu.Unlock()
			continue
		}
		op := applyMsg.Cmd
		msg := Msg{}
		msg.Cmd = op
		kv.mu.Lock()
		_, ok := kv.lastApplyIndex[op.Cid]
		if !ok {
			kv.lastApplyIndex[op.Cid] = 0
		}
		if kv.lastApplyIndex[op.Cid] < int(op.Seq) && applyMsg.CommandIndex > kv.lastIncludeIndex {
			kv.lastApplyIndex[op.Cid] = int(op.Seq)
			kv.lastIncludeIndex = applyMsg.CommandIndex
			switch op.Type {
			case "Put":
				kv.state[op.Key] = op.Value
			case "Append":
				kv.state[op.Key] += op.Value
			}
			DPrintf("server %v apply %v Cmd Tpye: %v,  cid: %v, seq: %v", kv.me, applyMsg.CommandIndex, op.Type, op.Cid, op.Seq)
			msg.YN = true
		} else {
			DPrintf("server %v discard %v Cmd Tpye: %v,  cid: %v, seq: %v", kv.me, applyMsg.CommandIndex, op.Type, op.Cid, op.Seq)
			msg.YN = false
		}
		kv.mu.Unlock()
		c, ok := kv.commitCh[applyMsg.CommandIndex]
		if ok {
			c <- msg
			DPrintf("server %v, key %v 放入chan", kv.me, op.Key)
		}

	}
}

func StartKVServer(servers []string, ip string, serverPort string, raftPort string, maxraftstate int) *KVServer {
	kv := new(KVServer)
	kv.me = ip + ":" + serverPort
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, ip+":"+raftPort, kv.applyCh)

	kv.state = make(map[string]string)
	kv.lastApplyIndex = make(map[int64]int)
	kv.commitCh = make(map[int]chan Msg)
	kv.lastIncludeIndex = 0

	go kv.readCommit()
	return kv
}

func (kv *KVServer) StartServer(port string) {
	// 开启raft服务器
	listen, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// 实例化grpc Server
	s := grpc.NewServer()

	kvpb.RegisterKVServer(s, kv)
	pb.RegisterRaftServer(s, kv.rf)

	log.Println("Listen on " + port)
	s.Serve(listen)
}
