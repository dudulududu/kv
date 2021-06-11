package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "../proto/raft"
	rfrpc "../rpc/raft"
)

const (
	Debug       = 0
	t_heartbeat = 50
	t_election  = 150
	t_commit    = 50
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("++"+format, a...)
	}
	return
}

type ApplyMsg struct {
	CommandValid bool
	Cmd          *pb.Command
	CommandIndex int
}

type Raft struct {
	mu               sync.Mutex
	peers            []string
	state            int32
	hear             bool
	me               string
	votedFor         string
	currentTerm      int32
	votecount        int32
	commitIndex      int32
	lastApplied      int32
	nextIndex        []int32
	matchIndex       []int32
	applyCh          chan ApplyMsg
	lastIncludeIndex int32
	lastIncludeTerm  int32
	log              []*pb.LogEntry
}

func (rf *Raft) AppendEntry(ctx context.Context, args *pb.AppendEntryArg) (*pb.AppendEntryReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := pb.AppendEntryReply{}
	//权限检验
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return &reply, nil
	}
	rf.hear = true
	if args.Term > rf.currentTerm {
		rf.reset(1, args.Term, "")
		// rf.persist()
	}
	//权限检验结束
	DPrintf("server %v 收到心跳", rf.me)

	//日志复制
	if args.PreLogIndex == rf.lastIncludeIndex && args.PreLogTerm == rf.lastIncludeTerm {
		rf.log = rf.log[:0]
		if args.Entries != nil && len(args.Entries) > 0 {
			for i := 0; i < len(args.Entries); i++ {
				rf.log = append(rf.log, args.Entries[i])
			}
		}
	} else {
		if int(args.PreLogIndex-rf.lastIncludeIndex) > len(rf.log) || rf.log[args.PreLogIndex-rf.lastIncludeIndex-1].Index != args.PreLogIndex || rf.log[args.PreLogIndex-rf.lastIncludeIndex-1].Term != args.PreLogTerm {
			if int(args.PreLogIndex-rf.lastIncludeIndex) <= len(rf.log) {
				// rf.log = rf.log[:args.PreLogIndex - rf.lastIncludeIndex]
				DPrintf("me: %v, preLogIndex: %v, preLogTerm: %v, rf.log[%v].Index: %v, rf.log[%v].Term: %v", rf.me, args.PreLogIndex, args.PreLogTerm, args.PreLogIndex, rf.log[args.PreLogIndex-rf.lastIncludeIndex-1].Index, args.PreLogIndex, rf.log[args.PreLogIndex-rf.lastIncludeIndex-1].Term)
				reply.ConflictTerm = rf.log[args.PreLogIndex-rf.lastIncludeIndex-1].Term
			} else {
				if len(rf.log) == 0 {
					reply.ConflictTerm = rf.lastIncludeTerm
				} else {
					reply.ConflictTerm = rf.log[len(rf.log)-1].Term
				}
			}
			reply.Success = false
			reply.Term = rf.currentTerm

			return &reply, nil
		}
		rf.log = rf.log[:args.PreLogIndex-rf.lastIncludeIndex]
		if args.Entries != nil && len(args.Entries) != 0 {
			for i := 0; i < len(args.Entries); i++ {
				rf.log = append(rf.log, args.Entries[i])
			}
		}

	}

	//更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int32(min(int(args.LeaderCommit), int(rf.log[len(rf.log)-1].Index)))
		DPrintf("%v update commiteIndex to %v, term: %v", rf.me, rf.commitIndex, rf.currentTerm)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	return &reply, nil
}

func (rf *Raft) background() {
	for {
		rf.mu.Lock()
		if !rf.hear && rf.state != 2 {
			rf.mu.Unlock()
			rf.election()
		} else {
			rf.hear = false
			rf.mu.Unlock()
		}
		n := t_election + rand.Intn(150)
		time.Sleep(time.Duration(n) * time.Millisecond)
	}
}

func (rf *Raft) commit() {
	for {
		rf.mu.Lock()
		//leader更新commitIndex
		if rf.state == 2 {
			N := int(rf.commitIndex - rf.lastIncludeIndex)
			for N < len(rf.log) && rf.log[N].Term != rf.currentTerm {
				N = N + 1
			}
			if N < len(rf.log) {
				count := 0
				nserver := len(rf.peers)
				for i := 0; i < nserver; i++ {
					if int(rf.matchIndex[i]) >= N+int(rf.lastIncludeIndex)+1 {
						count++
					}
				}
				if count+1 > (nserver+1)/2 {
					rf.commitIndex = int32(N) + rf.lastIncludeIndex + 1
					DPrintf("leader %v update commitIndex to %v", rf.me, rf.commitIndex)
				}
			}
		}

		//提交日志
		temp := []ApplyMsg{}
		if rf.commitIndex > rf.lastIncludeIndex {
			// flag++
			// DPrintf("server %v applied: %v, commitIndex: %v, lastIncludeIndex: %v, flag:%v", rf.me, rf.lastApplied, rf.commitIndex, rf.lastIncludeIndex, flag)
			for i := rf.lastApplied - rf.lastIncludeIndex; i <= rf.commitIndex-rf.lastIncludeIndex-1; i++ {
				msg := ApplyMsg{}
				msg.CommandValid = true
				msg.Cmd = rf.log[i].Cmd
				msg.CommandIndex = int(rf.log[i].Index)
				DPrintf("%v commit %v entries, command: %v", rf.me, rf.log[i].Index, rf.log[i].Cmd)
				temp = append(temp, msg)
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()

		for i := 0; i < len(temp); i++ {
			rf.applyCh <- temp[i]
		}
		//提交日志结束
		time.Sleep(time.Duration(t_commit) * time.Millisecond)
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.state = 0
	rf.votedFor = rf.me
	rf.currentTerm = rf.currentTerm + 1
	nserver := len(rf.peers)
	rf.votecount = 1

	args := pb.RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me

	if len(rf.log) == 0 {
		args.LastLogIndex = rf.lastIncludeIndex
		args.LastLogTerm = rf.lastIncludeTerm
	} else {
		lastLog := rf.log[len(rf.log)-1]
		args.LastLogIndex = lastLog.Index
		args.LastLogTerm = lastLog.Term
	}

	DPrintf("%v start election, lastTerm:%v, lastIndex: %v term : %v", rf.me, args.LastLogTerm, args.LastLogIndex, rf.currentTerm)
	rf.mu.Unlock()

	for i := 0; i < nserver; i++ {
		reply := pb.RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}

}

func (rf *Raft) heartbeat() {
	// rf.sendHeartbeat()
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == 2 {
			go rf.sendHeartbeat()
		}

		time.Sleep(time.Duration(t_heartbeat) * time.Millisecond)
	}

}

func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == 2
}

func Make(peers []string, me string, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.applyCh = applyCh
	rf.peers = peers
	rf.hear = false
	rf.me = me
	rand.Seed(time.Now().UnixNano())
	rf.reset(1, 0, "")
	rf.hear = false
	rf.matchIndex = make([]int32, len(peers))
	rf.nextIndex = make([]int32, len(peers))
	rf.log = make([]*pb.LogEntry, 0)
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	go rf.background()
	go rf.heartbeat()
	go rf.commit()
	return rf
}

func min(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}

func (rf *Raft) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := pb.RequestVoteReply{}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return &reply, nil
	}
	if args.Term > rf.currentTerm {
		rf.reset(1, args.Term, "")
		// rf.persist()
	}
	if rf.votedFor != "" {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return &reply, nil
	}
	var lastTerm int32
	var lastIndex int32
	if len(rf.log) == 0 {
		lastIndex = 0
		lastTerm = 0
	} else {
		lastLog := rf.log[len(rf.log)-1]
		lastIndex = lastLog.Index
		lastTerm = lastLog.Term
	}
	if lastTerm > args.LastLogTerm {
		DPrintf("%v recive %v request vote, but %v'log term smaller, term: %v, lastTerm: %v, args.lastTerm: %v, lastIndex: %v, args.lastIndex: %v", rf.me, args.CandidateId, args.CandidateId, rf.currentTerm, lastTerm, args.LastLogTerm, lastIndex, args.LastLogIndex)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return &reply, nil
	}
	if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		DPrintf("%v recive %v request vote, but %v's log smaller, term: %v, lastTerm: %v, args.lastTerm: %v, lastIndex: %v, args.lastIndex: %v", rf.me, args.CandidateId, args.CandidateId, rf.currentTerm, lastTerm, args.LastLogTerm, lastIndex, args.LastLogIndex)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return &reply, nil
	}
	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.hear = true
	// rf.persist()
	DPrintf("%v vote to %v, term:%v", rf.me, args.CandidateId, rf.currentTerm)
	return &reply, nil
}

func (rf *Raft) reset(state int32, currentTerm int32, votedFor string) {
	rf.state = state
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
}

func (rf *Raft) sendAppendEntry(server int, args *pb.AppendEntryArg, reply *pb.AppendEntryReply) bool {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if state != 2 {
		return false
	}
	err := rfrpc.Call(rf.peers[server], "AppendEntry", args, reply, "raft")

	if err != nil {
		// DPrintf("发生心跳失败")
		return false
	}
	// DPrintf("发生心跳成功")
	if !reply.Success {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.reset(1, reply.Term, "")
			// rf.persist()
			rf.mu.Unlock()
			return false
		}
		if rf.state != 2 {
			rf.mu.Unlock()
			return false
		}
		// TODO 更新nextIndex 和 matchIndex
		targetTerm := int32(min(int(args.PreLogTerm), int(reply.ConflictTerm)))
		preLogIndex := args.PreLogIndex
		var i int
		for i = 0; i < min(int(preLogIndex-rf.lastIncludeIndex-1), len(rf.log)-1); i++ {
			if rf.log[i].Term >= targetTerm {
				break
			}
		}
		var index int32
		if rf.log[i].Index == preLogIndex {
			index = preLogIndex
		} else {
			index = rf.log[i].Index + 1
		}
		rf.matchIndex[server] = -1
		rf.nextIndex[server] = index
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		if args.Entries == nil || len(args.Entries) == 0 {
			rf.matchIndex[server] = args.PreLogIndex
			rf.nextIndex[server] = args.PreLogIndex + 1
		} else {
			rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
		}
		rf.mu.Unlock()
	}
	return true
}

func (rf *Raft) sendHeartbeat() {
	nserver := len(rf.peers)

	for i := 0; i < nserver; i++ {
		rf.mu.Lock()
		var preLogIndex int32
		var preLogTerm int32
		preLogIndex = rf.nextIndex[i] - 1
		if len(rf.log) == 0 || preLogIndex == rf.lastIncludeIndex {
			preLogTerm = rf.lastIncludeTerm
		} else {
			preLogTerm = rf.log[preLogIndex-1].Term
		}
		if rf.nextIndex[i] > rf.lastIncludeIndex {
			var args pb.AppendEntryArg
			args.PreLogIndex = preLogIndex
			args.PreLogTerm = preLogTerm
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.Entries = rf.log[min(len(rf.log), int(rf.nextIndex[i]-rf.lastIncludeIndex-1)):len(rf.log)]
			args.LeaderCommit = rf.commitIndex
			DPrintf("%v send %v Entries to %v, preIndex: %v, term: %v, entries: %v", rf.me, len(args.Entries), i, args.PreLogIndex, rf.currentTerm, args.Entries)
			rf.mu.Unlock()
			reply := pb.AppendEntryReply{}
			go rf.sendAppendEntry(i, &args, &reply)
		} else {
			//需要同步
			// DPrintf("send snap to %v, matchIndex: %v, nextIndex: %v, lastIncludeIndex: %v", i, rf.matchIndex[i], rf.nextIndex[i], rf.lastIncludeIndex)
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) sendRequestVote(server int, args *pb.RequestVoteArgs, reply *pb.RequestVoteReply) bool {
	err := rfrpc.Call(rf.peers[server], "RequestVote", args, reply, "raft")
	if err != nil {
		return false
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.reset(1, reply.Term, "")
		rf.votecount = 0
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	if reply.VoteGranted {
		rf.mu.Lock()
		if reply.Term == rf.currentTerm {
			rf.votecount = rf.votecount + 1

			nserver := int32(len(rf.peers) + 1)
			if rf.votecount > nserver/2 {
				if rf.state != 2 {
					DPrintf("%v is leader ,term : %v", rf.me, rf.currentTerm)
					// TODO 初始化nextIndex 和 matchIndex
					for i := 0; i < len(rf.matchIndex); i++ {
						rf.matchIndex[i] = -1
						// rf.nextIndex[i] = len(rf.log) + rf.lastIncludeIndex
						if len(rf.log) > 0 {
							rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
						} else {
							rf.nextIndex[i] = rf.lastIncludeIndex + 1
						}
					}
					rf.state = 2
					rf.votecount = 0
					go rf.sendHeartbeat()
				}
			}
		}
		rf.mu.Unlock()
	}
	return true
}

func (rf *Raft) Start(cmd pb.Command) (int, int, bool) {
	realIndex := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	isLeader = rf.state == 2
	term = int(rf.currentTerm)

	if isLeader {
		realIndex = len(rf.log) + int(rf.lastIncludeIndex) + 1
		rf.log = append(rf.log, &pb.LogEntry{Cmd: &cmd, Term: int32(term), Index: int32(realIndex)})
		// rf.persist()
		DPrintf("%v accept client's log, len: %v, term: %v, index: %v, command: %v", rf.me, len(rf.log), term, realIndex, cmd)
	}

	rf.mu.Unlock()
	return realIndex, term, isLeader
}
