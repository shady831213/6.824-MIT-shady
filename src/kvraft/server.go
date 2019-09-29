package raftkv

import (
	"context"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OPCode string

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpCode   OPCode
	ServerId int
	ClerkId  int64
	SeqId    int
	Key      string
	Value    string
}

type KVRPCReq struct {
	OpCode OPCode
	args   interface{}
	reply  interface{}
	done   chan struct{}
}

type KVRPCResp struct {
	wrongLeader bool
	leader      int
	err         Err
	op          *Op
	rfIndex     int
	done        chan struct{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db         map[string]string
	clerkTrack map[int64]int
	ctx        context.Context
	cancel     func()
	rpc        chan KVRPCReq
	pending    *KVRPCResp
}

func (kv *KVServer) serveRPC(opcode OPCode, args interface{}, reply interface{}) {
	req := KVRPCReq{
		opcode,
		args,
		reply,
		make(chan struct{}),
	}
	kv.rpc <- req
	<-req.done
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.serveRPC(GET, args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.serveRPC((OPCode)(args.Op), args, reply)
}

func (kv *KVServer) waitingCommit(op *Op, index int) KVRPCResp {
	commit := KVRPCResp{
		true,
		kv.me,
		"",
		op,
		index,
		make(chan struct{}),
	}
	kv.mu.Lock()
	kv.pending = &commit
	kv.mu.Unlock()
	DPrintf("Waiting %s commitProcess me: %d %+v", op.OpCode, kv.me, op)
	<-commit.done
	return commit
}

func (kv *KVServer) handleRPC(req *KVRPCReq) {
	switch req.OpCode {
	case GET:
		args, reply := req.args.(*GetArgs), req.reply.(*GetReply)
		reply.Server = kv.me
		DPrintf("get Get me: %d %+v %+v", kv.me, args, reply)
		//if seqId, ok := kv.clerkTrack[args.ClerkId]; ok && args.SeqId <= seqId {
		//	DPrintf("ignore Get me: %d %+v %+v seqID:%d", kv.me, args, reply,seqId)
		//	return
		//}
		op := Op{GET, kv.me, args.ClerkId, args.SeqId, args.Key, ""}
		index, _, isLeader, leader := kv.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Get me: %d %+v %+v", kv.me, args, reply)
			return
		}
		commit := kv.waitingCommit(&op, index)
		reply.Err = commit.err
		reply.WrongLeader = commit.wrongLeader
		reply.Leader = commit.leader
		reply.Value = commit.op.Value
		DPrintf("reply Get me: %d %+v %+v", kv.me, args, reply)
		break
	case PUT, APPEND:
		args, reply := req.args.(*PutAppendArgs), req.reply.(*PutAppendReply)
		reply.Server = kv.me
		kv.mu.Lock()
		DPrintf("get PutAppend me: %d %+v %+v seqId: %d", kv.me, args, reply, kv.clerkTrack[args.ClerkId])
		if seqId, ok := kv.clerkTrack[args.ClerkId]; ok && args.SeqId <= seqId {
			DPrintf("ignore PutAppend me: %d %+v %+v seqId:%d", kv.me, args, reply, seqId)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		op := Op{(OPCode)(args.Op), kv.me, args.ClerkId, args.SeqId, args.Key, args.Value}
		index, _, isLeader, leader := kv.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader PutAppend me: %d %+v %+v", kv.me, args, reply)
			return
		}
		commit := kv.waitingCommit(&op, index)
		reply.Err = commit.err
		reply.WrongLeader = commit.wrongLeader
		reply.Leader = commit.leader
		DPrintf("reply PutAppend me: %d %+v %+v", kv.me, args, reply)
		break
	}
}

func (kv *KVServer) rpcProcess() {
	for {
		select {
		case rpc := <-kv.rpc:
			kv.handleRPC(&rpc)
			rpc.done <- struct{}{}
			break
		case <-kv.ctx.Done():
			return
		}
	}
}

func (kv *KVServer) execute(op *Op) Err {
	switch op.OpCode {
	case PUT:
		kv.db[op.Key] = op.Value
		break
	case GET:
		v, exist := kv.db[op.Key]
		if !exist {
			return ErrNoKey
		}
		op.Value = v
		break
	case APPEND:

		if v, exist := kv.db[op.Key]; !exist {
			kv.db[op.Key] = op.Value
		} else {
			kv.db[op.Key] = v + op.Value
		}
		break
	}
	return OK
}

func (kv *KVServer) servePendingRPC(op *Op, index int, err Err) {
	kv.mu.Lock()
	kv.clerkTrack[op.ClerkId] = op.SeqId
	if kv.pending != nil && index == kv.pending.rfIndex {
		DPrintf("commitProcess me: %d %+v %+v Index:%d", kv.me, op, kv.pending, index)
		kv.pending.wrongLeader = op.SeqId != kv.pending.op.SeqId || op.ClerkId != kv.pending.op.ClerkId
		kv.pending.leader = op.ServerId
		kv.pending.err = err
		kv.pending.op = op
		close(kv.pending.done)
		kv.pending = nil
	}
	kv.mu.Unlock()
}

func (kv *KVServer) commitProcess() {
	for {
		select {
		case apply := <-kv.applyCh:
			if apply.CommandValid {
				op, _ := (apply.Command).(Op)
				err := kv.execute(&op)
				DPrintf("server%d apply %+v Index:%d", kv.me, op, apply.CommandIndex)
				kv.servePendingRPC(&op, apply.CommandIndex, err)
			}
			break
		case <-kv.ctx.Done():
			kv.mu.Lock()
			if kv.pending != nil {
				close(kv.pending.done)
				kv.pending = nil
			}
			kv.mu.Unlock()
			return
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.cancel()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.clerkTrack = make(map[int64]int)
	kv.rpc = make(chan KVRPCReq)
	kv.pending = nil

	//kv.commitProcess = make(chan KVRPCReq)
	kv.ctx, kv.cancel = context.WithCancel(context.Background())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.commitProcess()
	go kv.rpcProcess()

	return kv
}
