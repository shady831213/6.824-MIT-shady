package shardmaster

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"raft"
	"strconv"
)
import "labrpc"
import "sync"
import "labgob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OPCode string

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type ClerkTrackAction int

const (
	_ ClerkTrackAction = iota
	ClerkOK
	ClerkIgnore
	ClerkRetry
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxraftstate int // snapshot if log grows this big
	booting      bool
	ClerkTrack   map[int64]int
	ctx          context.Context
	cancel       func()
	issueing     chan SMRPCIssueItem
	committing   chan SMRPCCommitItem
	pendingIndex int
	configs      []Config // indexed by config num
	chash        CHash
}

type Op struct {
	// Your data here.
	OpCode   OPCode
	ServerId int
	ClerkId  int64
	SeqId    int
	Value    []byte
}

type smRPCItem struct {
	op   *Op
	resp func(SMRPCResp)
	done chan struct{}
}

type SMRPCIssueItem struct {
	smRPCItem
	preIssueCheck      func() bool
	wrongLeaderHandler func(int)
}

type SMRPCCommitItem struct {
	smRPCItem
}

type SMRPCResp struct {
	wrongLeader bool
	leader      int
	err         Err
	value       interface{}
}

func (sm *ShardMaster) checkClerkTrack(clerkId int64, sedId int) ClerkTrackAction {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	v, ok := sm.ClerkTrack[clerkId]
	//when restart
	if !ok && sedId > 0 || sedId > v+1 {
		return ClerkRetry
	}
	//for restart corner case
	if !ok && sedId == 0 || sedId == v+1 {
		if sm.booting {
			sm.booting = false
			return ClerkRetry
		}
		return ClerkOK
	}
	return ClerkIgnore
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	issue := SMRPCIssueItem{
		smRPCItem{&Op{JOIN, sm.me, args.ClerkId, args.SeqId, func() []byte {
			s := new(bytes.Buffer)
			e := labgob.NewEncoder(s)
			e.Encode(args.Servers)
			return s.Bytes()
		}()},
			func(resp SMRPCResp) {
				reply.Server = sm.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply Join me: %d %+v %+v", sm.me, args, reply)
			},
			make(chan struct{})},

		func() bool {
			switch sm.checkClerkTrack(args.ClerkId, args.SeqId) {
			case ClerkIgnore:
				DPrintf("ignore Join me: %d %+v %+v", sm.me, args, reply)
				return false
			case ClerkRetry:
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry Join me: %d %+v %+v", sm.me, args, reply)
				return false
			}
			return true
		},
		func(leader int) {
			reply.Server = sm.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Join me: %d %+v %+v", sm.me, args, reply)
		},
	}
	sm.issueing <- issue
	<-issue.done
	DPrintf("reply Join done me: %d %+v", sm.me, issue.op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	issue := SMRPCIssueItem{
		smRPCItem{&Op{LEAVE, sm.me, args.ClerkId, args.SeqId,
			func() []byte {
				s := new(bytes.Buffer)
				e := labgob.NewEncoder(s)
				e.Encode(args.GIDs)
				return s.Bytes()
			}()},
			func(resp SMRPCResp) {
				reply.Server = sm.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply Leave me: %d %+v %+v", sm.me, args, reply)
			},
			make(chan struct{})},

		func() bool {
			switch sm.checkClerkTrack(args.ClerkId, args.SeqId) {
			case ClerkIgnore:
				DPrintf("ignore Leave me: %d %+v %+v", sm.me, args, reply)
				return false
			case ClerkRetry:
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry Leave me: %d %+v %+v", sm.me, args, reply)
				return false
			}
			return true
		},

		func(leader int) {
			reply.Server = sm.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Leave me: %d %+v %+v", sm.me, args, reply)
		},
	}
	sm.issueing <- issue
	<-issue.done
	DPrintf("reply Leave done me: %d %+v", sm.me, issue.op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	issue := SMRPCIssueItem{
		smRPCItem{&Op{MOVE, sm.me, args.ClerkId, args.SeqId, func() []byte {
			s := new(bytes.Buffer)
			e := labgob.NewEncoder(s)
			e.Encode(args.Shard)
			e.Encode(args.GID)
			return s.Bytes()
		}()},
			func(resp SMRPCResp) {
				reply.Server = sm.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply Move me: %d %+v %+v", sm.me, args, reply)
			},
			make(chan struct{})},

		func() bool {
			switch sm.checkClerkTrack(args.ClerkId, args.SeqId) {
			case ClerkIgnore:
				DPrintf("ignore Move me: %d %+v %+v", sm.me, args, reply)
				return false
			case ClerkRetry:
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry Move me: %d %+v %+v", sm.me, args, reply)
				return false
			}
			return true
		},

		func(leader int) {
			reply.Server = sm.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Move me: %d %+v %+v", sm.me, args, reply)
		},
	}
	sm.issueing <- issue
	<-issue.done
	DPrintf("reply PutAppend done me: %d %+v", sm.me, issue.op)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	issue := SMRPCIssueItem{
		smRPCItem{&Op{QUERY, sm.me, args.ClerkId, args.SeqId, func() []byte {
			s := new(bytes.Buffer)
			e := labgob.NewEncoder(s)
			e.Encode(args.Num)
			return s.Bytes()
		}()},
			func(resp SMRPCResp) {
				reply.Server = sm.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				reply.Config = resp.value.(Config)
				DPrintf("reply Query me: %d %+v %+v", sm.me, args, reply)
			},
			make(chan struct{})},

		func() bool {
			return true
		},
		func(leader int) {
			reply.Server = sm.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Query me: %d %+v %+v", sm.me, args, reply)
		},
	}
	sm.issueing <- issue
	<-issue.done
	DPrintf("reply Query done me: %d %+v", sm.me, issue.op)
}

func (sm *ShardMaster) issue(item SMRPCIssueItem) {
	if !item.preIssueCheck() {
		return
	}

	index, _, isLeader, leader := sm.rf.Start(*item.op)
	if !isLeader {
		item.wrongLeaderHandler(leader)
		return

	}
	commit := SMRPCCommitItem{
		smRPCItem{
			item.op,
			item.resp,
			make(chan struct{}),
		},
	}
	sm.mu.Lock()
	sm.pendingIndex = index
	sm.mu.Unlock()
	sm.committing <- commit
	DPrintf("Waiting commitProcess me: %d %+v", sm.me, item.op)
	<-commit.done
}

func (sm *ShardMaster) issueProcess() {
	for {
		select {
		case item := <-sm.issueing:
			sm.issue(item)
			item.done <- struct{}{}
			DPrintf("issue done me: %d %+v", sm.me, item.op)
			break
		case <-sm.ctx.Done():
			return
		}
	}
}

func (sm *ShardMaster) allocConfig() *Config {
	num := len(sm.configs)
	old := sm.configs[num-1]
	shards := [NShards]int{}
	groups := make(map[int][]string)
	for i, v := range old.Shards {
		shards[i] = v
	}
	for k, v := range old.Groups {
		groups[k] = v
	}
	sm.configs = append(sm.configs, Config{
		num,
		shards,
		groups,
	})
	return &sm.configs[num]
}

func (sm *ShardMaster) execute(op *Op) (interface{}, Err) {
	switch op.OpCode {
	case JOIN:
		servers := make(map[int][]string)
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&servers); e != nil {
			panic(e)
		}
		config := sm.allocConfig()
		//fixme:may be set vnode number corresponding server number
		for k, v := range servers {
			config.Groups[k] = v
			sm.chash.AddNode(strconv.Itoa(k),len(v))
		}
		for i := range config.Shards {
			gs := sm.chash.GetNode(strconv.Itoa(i))
			g, _ := strconv.Atoi(gs)
			config.Shards[i] = g
		}
		DPrintf("execute Join me: %d %+v %+v", sm.me, op, sm.configs)
		break
	case LEAVE:
		gids := make([]int, 0)
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&gids); e != nil {
			panic(e)
		}
		config := sm.allocConfig()
		shards := make([]int, 0)
		for _, g := range gids {
			for i, s := range config.Shards {
				if s == g {
					shards = append(shards, i)
				}
			}
			delete(config.Groups, g)
			sm.chash.RemoveNode(strconv.Itoa(g))
		}
		for _,i := range shards {
			g, _ := strconv.Atoi(sm.chash.GetNode(strconv.Itoa(i)))
			config.Shards[i] = g
		}
		DPrintf("execute Leave me: %d %+v %+v", sm.me, op, sm.configs)
		break
	case MOVE:
		shard, gid := 0, 0
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&shard); e != nil {
			panic(e)
		}
		if e := d.Decode(&gid); e != nil {
			panic(e)
		}
		if _, ok := sm.configs[len(sm.configs)-1].Groups[gid]; !ok {
			return nil, Err(fmt.Sprintf("gid %d does not exist!", gid))
		}
		if shard >= NShards {
			return nil, Err(fmt.Sprintf("shard %d does not exist!", shard))
		}
		config := sm.allocConfig()
		config.Shards[shard] = gid
		break
	case QUERY:
		num := 0
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&num); e != nil {
			panic(e)
		}
		DPrintf("execute Query me: %d %+v %+v", sm.me, op, sm.configs)
		if num < 0 {
			return sm.configs[len(sm.configs)-1], OK
		}
		if num > len(sm.configs)-1 {
			return sm.configs[len(sm.configs)-1], Err(fmt.Sprintf("config %d does not exist!", num))
		}
		return sm.configs[num], OK
	}
	return nil, OK
}

func (sm *ShardMaster) servePendingRPC(apply *raft.ApplyMsg, err Err, value interface{}) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if apply.CommandIndex == sm.pendingIndex {
		item := <-sm.committing
		op, ok := (apply.Command).(Op)
		DPrintf("commitProcess me: %d %+v %+v Index:%d", sm.me, op, item.op, apply.CommandIndex)
		item.resp(SMRPCResp{
			op.SeqId != item.op.SeqId || op.ClerkId != item.op.ClerkId || !ok || !apply.CommandValid,
			op.ServerId,
			err,
			value,
		})
		close(item.done)
		return
	}
	if apply.CommandIndex > sm.pendingIndex {
		select {
		case item := <-sm.committing:
			DPrintf("commitProcess me: %d  %+v Index:%d", sm.me, item.op, apply.CommandIndex)
			item.resp(SMRPCResp{
				true,
				sm.me,
				err,
				value,
			})
			close(item.done)

		default:

		}
	}

}

func (sm *ShardMaster) updateClerkTrack(clientId int64, seqId int) {
	sm.mu.Lock()
	sm.ClerkTrack[clientId] = seqId
	sm.mu.Unlock()
}

func (sm *ShardMaster) decodeSnapshot(snapshot []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&sm.configs); e != nil {
		panic(e)
	}
	if e := d.Decode(&sm.ClerkTrack); e != nil {
		panic(e)
	}
	for g, _ := range sm.configs[len(sm.configs)-1].Groups {
		sm.chash.AddNode(strconv.Itoa(g), 1)
	}
}

func (sm *ShardMaster) encodeSnapshot() []byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	s := new(bytes.Buffer)
	e := labgob.NewEncoder(s)
	e.Encode(sm.configs)
	e.Encode(sm.ClerkTrack)
	return s.Bytes()
}

func (sm *ShardMaster) commitProcess() {
	for {
		select {
		case apply := <-sm.applyCh:
			var err Err
			var value interface{}
			if apply.CommandValid {
				op, _ := (apply.Command).(Op)
				value, err = sm.execute(&op)
				sm.updateClerkTrack(op.ClerkId, op.SeqId)
				DPrintf("server%d apply %+v Index:%d", sm.me, op, apply.CommandIndex)
			}
			if apply.Snapshot {
				snapshot, _ := (apply.Command).([]byte)
				//DPrintf("install snapshot before decode me: %d %+v", sm.me, sm.DB)
				sm.decodeSnapshot(snapshot)
			} else {
				sm.servePendingRPC(&apply, err, value)
				if apply.StageSize >= sm.maxraftstate && sm.maxraftstate > 0 {
					//DPrintf("make snapshot me: %d Index:%d stageSize %d %+v", kv.me, apply.CommandIndex, apply.StageSize, kv.DB)
					sm.rf.Snapshot(apply.CommandIndex, sm.encodeSnapshot())
				}
			}
			DPrintf("server%d apply Index:%d done", sm.me, apply.CommandIndex)
			break
		case <-sm.ctx.Done():
			return
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	DPrintf("server%d killed", sm.me)
	sm.rf.Kill()
	// Your code here, if desired.
	sm.cancel()
	select {
	case item := <-sm.issueing:
		close(item.done)
	default:
	}
	select {
	case item := <-sm.committing:
		close(item.done)
	default:
	}
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh, true)

	// Your code here.
	sm.booting = true
	sm.chash = CHash{}
	sm.chash.Init(md5.New(), 128)
	sm.ClerkTrack = make(map[int64]int)
	sm.issueing = make(chan SMRPCIssueItem)
	sm.committing = make(chan SMRPCCommitItem, 1)
	sm.ctx, sm.cancel = context.WithCancel(context.Background())
	sm.pendingIndex = 0
	// You may need initialization code here.
	go sm.commitProcess()
	go sm.issueProcess()
	DPrintf("server%d start", sm.me)
	return sm
}
