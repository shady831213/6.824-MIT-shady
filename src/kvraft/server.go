package raftkv

import (
	"bytes"
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

/*
	rpc                             issue                         commit
-----------------------------------------------------------------------------------
	issueitem
		-resp() -------------------------------------------------------
	GET       --                                                      |
			    | --> issueing -> issue to raft --> committing -> call resp -> done
	PUTAPPEND --                                                                |
 reply <--------------issuedone<--------commit done <----------------------------
*/

type OPCode string

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type ClerkTrackAction int

const (
	_ ClerkTrackAction = iota
	ClerkOK
	ClerkIgnore
	ClerkRetry
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

type kvRPCItem struct {
	op   *Op
	resp func(KVRPCResp)
	done chan struct{}
}

type KVRPCIssueItem struct {
	kvRPCItem
	preIssueCheck      func() bool
	wrongLeaderHandler func(int)
}

type KVRPCCommitItem struct {
	kvRPCItem
}

type KVRPCResp struct {
	wrongLeader bool
	leader      int
	err         Err
	value       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	booting    bool
	DB         map[string]string
	ClerkTrack map[int64]int
	ctx        context.Context
	cancel     func()
	issueing   chan KVRPCIssueItem
	committing chan KVRPCCommitItem
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{GET, kv.me, args.ClerkId, args.SeqId, args.Key, ""},
			func(resp KVRPCResp) {
				reply.Server = kv.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				reply.Value = resp.value
				DPrintf("reply Get me: %d %+v %+v", kv.me, args, reply)
			},
			make(chan struct{})},

		func() bool {
			return true
		},
		func(leader int) {
			reply.Server = kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Get me: %d %+v %+v", kv.me, args, reply)
		},
	}
	kv.issueing <- issue
	<-issue.done
}

func (kv *KVServer) checkClerkTrack(clerkId int64, sedId int) ClerkTrackAction {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.ClerkTrack[clerkId]
	//when restart
	if !ok && sedId > 0 || sedId > v+1 {
		return ClerkRetry
	}
	//for restart corner case
	if !ok && sedId == 0 || sedId == v+1 {
		if kv.booting {
			kv.booting = false
			return ClerkRetry
		}
		return ClerkOK
	}
	return ClerkIgnore
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{(OPCode)(args.Op), kv.me, args.ClerkId, args.SeqId, args.Key, args.Value},
			func(resp KVRPCResp) {
				reply.Server = kv.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply PutAppend me: %d %+v %+v", kv.me, args, reply)
			},
			make(chan struct{})},

		func() bool {
			switch kv.checkClerkTrack(args.ClerkId, args.SeqId) {
			case ClerkIgnore:
				DPrintf("ignore PutAppend me: %d %+v %+v", kv.me, args, reply)
				return false
			case ClerkRetry:
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry PutAppend me: %d %+v %+v", kv.me, args, reply)
				return false
			}
			return true
		},

		func(leader int) {
			reply.Server = kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader PutAppend me: %d %+v %+v", kv.me, args, reply)
		},
	}
	kv.issueing <- issue
	<-issue.done
}

func (kv *KVServer) issue(item KVRPCIssueItem) {
	if !item.preIssueCheck() {
		return
	}
	if _, _, isLeader, leader := kv.rf.Start(*item.op); !isLeader {
		item.wrongLeaderHandler(leader)
		return
	}
	commit := KVRPCCommitItem{
		kvRPCItem{
			item.op,
			item.resp,
			make(chan struct{}),
		},
	}
	kv.committing <- commit
	DPrintf("Waiting commitProcess me: %d %+v", kv.me, item)
	<-commit.done
}

func (kv *KVServer) issueProcess() {
	for {
		select {
		case item := <-kv.issueing:
			kv.issue(item)
			item.done <- struct{}{}
			break
		case <-kv.ctx.Done():
			return
		}
	}
}

func (kv *KVServer) execute(op *Op) (string, Err) {
	switch op.OpCode {
	case PUT:
		kv.DB[op.Key] = op.Value
		break
	case GET:
		v, exist := kv.DB[op.Key]
		if !exist {
			return "", ErrNoKey
		}
		return v, OK
	case APPEND:

		if v, exist := kv.DB[op.Key]; !exist {
			kv.DB[op.Key] = op.Value
		} else {
			kv.DB[op.Key] = v + op.Value
		}
		break
	}
	return "", OK
}

func (kv *KVServer) servePendingRPC(apply *raft.ApplyMsg, err Err, value string) {
	select {
	case item := <-kv.committing:
		op, ok := (apply.Command).(Op)
		DPrintf("commitProcess me: %d %+v %+v Index:%d", kv.me, op, item, apply.CommandIndex)
		item.resp(KVRPCResp{
			op.SeqId != item.op.SeqId || op.ClerkId != item.op.ClerkId || !ok || !apply.CommandValid,
			op.ServerId,
			err,
			value,
		})
		close(item.done)
	default:
	}

}

func (kv *KVServer) updateClerkTrack(clerkId int64, seqId int) {
	kv.mu.Lock()
	kv.ClerkTrack[clerkId] = seqId
	kv.mu.Unlock()
}

func (kv *KVServer) decodeSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&kv.DB); e != nil {
		panic(e)
	}
	if e := d.Decode(&kv.ClerkTrack); e != nil {
		panic(e)
	}
}

func (kv *KVServer) encodeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	s := new(bytes.Buffer)
	e := labgob.NewEncoder(s)
	e.Encode(kv.DB)
	e.Encode(kv.ClerkTrack)
	return s.Bytes()
}

func (kv *KVServer) commitProcess() {
	for {
		select {
		case apply := <-kv.applyCh:
			var err Err
			var value string
			if apply.CommandValid {
				op, _ := (apply.Command).(Op)
				value, err = kv.execute(&op)
				kv.updateClerkTrack(op.ClerkId, op.SeqId)
				DPrintf("server%d apply %+v Index:%d", kv.me, op, apply.CommandIndex)
			}
			if apply.Snapshot {
				snapshot, _ := (apply.Command).([]byte)
				kv.decodeSnapshot(snapshot)
				select {
				case item := <-kv.committing:
					DPrintf("retry because install snapshot me: %d %+v Index:%d", kv.me, item, apply.CommandIndex)
					item.resp(KVRPCResp{
						true,
						-1,
						err,
						value,
					})
					close(item.done)
				default:
				}
			} else {
				kv.servePendingRPC(&apply, err, value)
				if apply.StageSize >= kv.maxraftstate && kv.maxraftstate > 0 {
					DPrintf("make snapshot me: %d Index:%d stageSize %d", kv.me, apply.CommandIndex, apply.StageSize)
					kv.rf.Snapshot(apply.CommandIndex, kv.encodeSnapshot())
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
	select {
	case item := <-kv.issueing:
		close(item.done)
	default:
	}
	select {
	case item := <-kv.committing:
		close(item.done)
	default:
	}
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
	kv.booting = true
	kv.DB = make(map[string]string)
	kv.ClerkTrack = make(map[int64]int)
	kv.issueing = make(chan KVRPCIssueItem)
	kv.committing = make(chan KVRPCCommitItem, 1)
	kv.ctx, kv.cancel = context.WithCancel(context.Background())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, true)
	snapshot := kv.rf.GetSnapshot()
	if snapshot.Index != 0 {
		kv.decodeSnapshot(snapshot.Data)
	}
	// You may need initialization code here.
	go kv.commitProcess()
	go kv.issueProcess()

	return kv
}
