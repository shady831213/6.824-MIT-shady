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
	Leader      int
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
	db             map[string]string
	clerkTrack     map[int64]int
	ctx            context.Context
	cancel         func()
	commit         chan KVCommit
	pendingRPC     chan struct{}
	backToFollower chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("get Get before lock me: %d %+v %+v", kv.me, args, reply)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Server = kv.me
	DPrintf("get Get me: %d %+v %+v seqId: %d", kv.me, args, reply, kv.clerkTrack[args.ClerkId])
	//if seqId, ok := kv.clerkTrack[args.ClerkId]; ok && args.SeqId <= seqId {
	//	DPrintf("ignore Get me: %d %+v %+v seqID:%d", kv.me, args, reply,seqId)
	//	return
	//}
	op := Op{GET, kv.me, args.ClerkId, args.SeqId, args.Key, ""}
	_, _, isLeader, leader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Leader = leader
		DPrintf("NotLeader Get me: %d %+v %+v", kv.me, args, reply)
		return
	}
	kv.pendingRPC <- struct{}{}
	DPrintf("Waiting Get commit me: %d %+v %+v", kv.me, args, reply)
	commit := <-kv.commit
	reply.Err = commit.Err
	reply.WrongLeader = commit.WrongLeader
	reply.Leader = commit.Leader
	if commit.op != nil {
		reply.Value = commit.op.Value
		kv.clerkTrack[op.ClerkId] = op.SeqId
	}
	DPrintf("reply Get me: %d %+v %+v", kv.me, args, reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("get PutAppend before lock me: %d %+v %+v", kv.me, args, reply)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Server = kv.me
	DPrintf("get PutAppend me: %d %+v %+v seqId: %d", kv.me, args, reply, kv.clerkTrack[args.ClerkId])
	if seqId, ok := kv.clerkTrack[args.ClerkId]; ok && args.SeqId <= seqId {
		DPrintf("ignore PutAppend me: %d %+v %+v seqId:%d", kv.me, args, reply, seqId)
		return
	}
	op := Op{(OPCode)(args.Op), kv.me, args.ClerkId, args.SeqId, args.Key, args.Value}
	_, _, isLeader, leader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Leader = leader
		DPrintf("NotLeader PutAppend me: %d %+v %+v", kv.me, args, reply)
		return
	}
	kv.pendingRPC <- struct{}{}
	DPrintf("Waiting PutAppend commit me: %d %+v %+v", kv.me, args, reply)
	commit := <-kv.commit
	reply.Err = commit.Err
	reply.WrongLeader = commit.WrongLeader
	reply.Leader = commit.Leader
	if commit.op != nil {
		kv.clerkTrack[op.ClerkId] = op.SeqId
	}
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
				select {
				case <-kv.pendingRPC:
					kv.commit <- KVCommit{
						op.ServerId != kv.me,
						op.ServerId,
						err,
						&op,
					}
				default:
					kv.mu.Lock()
					kv.clerkTrack[op.ClerkId] = op.SeqId
					kv.mu.Unlock()
				}
				DPrintf("commit me: %d %+v Index:%d", kv.me, op, apply.CommandIndex)
			}
			break
		case <-kv.backToFollower:
			DPrintf("back to follower me: %d", kv.me, )
			select {
			case <-kv.pendingRPC:
				DPrintf("release me: %d", kv.me, )
				kv.commit <- KVCommit{
					true,
					-1,
					OK,
					nil,
				}
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
	kv.pendingRPC = make(chan struct{}, 1)
	kv.commit = make(chan KVCommit)
	kv.ctx, kv.cancel = context.WithCancel(context.Background())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.backToFollower = make(chan struct{}, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, kv.backToFollower)

	// You may need initialization code here.
	go kv.applyMap()

	return kv
}
