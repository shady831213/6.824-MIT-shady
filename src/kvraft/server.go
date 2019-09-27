package raftkv

import (
	"context"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

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

type KVCommit struct {
	WrongLeader bool
	Err         Err
	op          *Op
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db           map[string]string
	clerkTrack   map[int64]int
	ctx          context.Context
	cancel       func()
	commit       chan KVCommit
	pendingIndex []int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	//if seqId, ok := kv.clerkTrack[args.ClerkId]; ok && args.SeqId <= seqId {
	//	DPrintf("ignore Get me: %d %+v %+v seqID:%d", kv.me, args, reply,seqId)
	//	kv.mu.Unlock()
	//	return
	//}
	op := Op{GET, kv.me, args.ClerkId, args.SeqId, args.Key, ""}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.pendingIndex = append(kv.pendingIndex, index)
	kv.mu.Unlock()
	commit := <-kv.commit
	reply.Err = commit.Err
	reply.WrongLeader = commit.WrongLeader
	reply.Value = commit.op.Value
	DPrintf("reply Get me: %d %+v %+v", kv.me, args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("get PutAppend me: %d %+v %+v seqId: %d", kv.me, args, reply, kv.clerkTrack[args.ClerkId])
	if seqId, ok := kv.clerkTrack[args.ClerkId]; ok && args.SeqId <= seqId {
		DPrintf("ignore PutAppend me: %d %+v %+v seqId:%d", kv.me, args, reply, seqId)
		kv.mu.Unlock()
		return
	}
	op := Op{(OPCode)(args.Op), kv.me, args.ClerkId, args.SeqId, args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.pendingIndex = append(kv.pendingIndex, index)
	kv.mu.Unlock()
	commit := <-kv.commit
	reply.Err = commit.Err
	reply.WrongLeader = commit.WrongLeader
	DPrintf("reply PutAppend me: %d %+v %+v", kv.me, args, reply)
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

func (kv *KVServer) applyMap() {
	for {
		select {
		case apply := <-kv.applyCh:
			if apply.CommandValid {
				op, _ := (apply.Command).(Op)
				err := kv.execute(&op)
				kv.mu.Lock()
				if len(kv.pendingIndex) != 0 && kv.pendingIndex[0] == apply.CommandIndex {
					kv.commit <- KVCommit{
						op.ServerId != kv.me,
						err,
						&op,
					}
					kv.pendingIndex = kv.pendingIndex[1:]
				}
				kv.clerkTrack[op.ClerkId] = op.SeqId
				kv.mu.Unlock()
			}
			break
		case <-kv.ctx.Done():
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
	kv.pendingIndex = make([]int, 0)
	kv.commit = make(chan KVCommit)
	kv.ctx, kv.cancel = context.WithCancel(context.Background())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyMap()

	return kv
}
