package shardkv

import (
	"bytes"
	"context"
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

type OPCode string

const (
	GET         = "Get"
	PUT         = "Put"
	APPEND      = "Append"
	GETSHARD    = "GetShard"
	UPDATESHARD = "UpdateShard"
	CONFIG      = "Config"
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
	Value    []byte
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
	value       interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	servers      []*labrpc.ClientEnd
	ck           *Clerk
	sm           *shardmaster.Clerk
	config       shardmaster.Config
	booting      bool
	DB           map[string]string
	ClerkTrack   map[int64]int
	ctx          context.Context
	cancel       func()
	issueing     chan KVRPCIssueItem
	committing   chan KVRPCCommitItem
	pendingIndex int
}

func (kv *ShardKV) checkClerkTrack(clerkId int64, sedId int) ClerkTrackAction {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.ClerkTrack[clerkId]
	DPrintf("checkClerkTrack me: %d gid: %d clerkId:%v, ok:%v seqId:%d, v:%d, %v", kv.me, kv.gid, clerkId, ok, sedId, v, kv.ClerkTrack)
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

func (kv *ShardKV) checkGroup(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.config.Groups[kv.gid]
	if key != "" {
		shard := key2shard(key)
		return kv.config.Shards[shard] == kv.gid && ok
	}
	return ok
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{GET, kv.me, args.ClerkId, args.SeqId,
			func() []byte {
				s := new(bytes.Buffer)
				e := labgob.NewEncoder(s)
				e.Encode(args.Key)
				return s.Bytes()
			}(),},
			func(resp KVRPCResp) {
				reply.Server = kv.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				reply.Value = resp.value.(string)
				DPrintf("reply Get me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
			},
			make(chan struct{})},

		func() bool {
			if !kv.checkGroup(args.Key) {
				reply.Err = ErrWrongGroup
				DPrintf("wrongGroup Get me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
				return false
			}
			return true
		},
		func(leader int) {
			reply.Server = kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Get me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
		},
	}
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply Get done me: %d gid: %d %+v", kv.me, kv.gid, issue.op)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{(OPCode)(args.Op), kv.me, args.ClerkId, args.SeqId,
			func() []byte {
				s := new(bytes.Buffer)
				e := labgob.NewEncoder(s)
				e.Encode(args.Key)
				e.Encode(args.Value)
				return s.Bytes()
			}(),},
			func(resp KVRPCResp) {
				reply.Server = kv.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply PutAppend me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
			},
			make(chan struct{})},

		func() bool {
			switch kv.checkClerkTrack(args.ClerkId, args.SeqId) {
			case ClerkIgnore:
				DPrintf("ignore PutAppend me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
				return false
			case ClerkRetry:
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry PutAppend me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
				return false
			}
			if !kv.checkGroup(args.Key) {
				reply.Err = ErrWrongGroup
				DPrintf("wrongGroup PutAppend me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
				return false
			}
			return true
		},

		func(leader int) {
			reply.Server = kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader PutAppend me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
		},
	}
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply PutAppend done me: %d gid: %d %+v", kv.me, kv.gid, issue.op)
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{GETSHARD, kv.me, -1, args.ConfigNum,
			func() []byte {
				s := new(bytes.Buffer)
				e := labgob.NewEncoder(s)
				e.Encode(args.Shard)
				return s.Bytes()
			}(),},
			func(resp KVRPCResp) {
				reply.Server = kv.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				reply.Value = resp.value.(map[string]string)
				DPrintf("reply GetMigrate me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
				//fmt.Printf("reply Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			kv.mu.Lock()
			defer kv.mu.Lock()
			if args.ConfigNum <= kv.config.Num {
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry GetMigrate me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
				//fmt.Printf("retry GetMigrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)
				return false
			}
			return true
		},

		func(leader int) {
			reply.Server = kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader GetMigrate me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
			//fmt.Printf("NotLeader Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply GetMigrate done me: %d gid: %d %+v", kv.me, kv.gid, issue.op)
	//fmt.Printf("reply Migrate done me: %d gid: %d %+v\n", kv.me, kv.gid, issue.op)
}

func (kv *ShardKV) UpdateShard(args UpdateShardArgs, reply *UpdateShardReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{UPDATESHARD, kv.me, -1, args.ConfigNum,
			func() []byte {
				s := new(bytes.Buffer)
				e := labgob.NewEncoder(s)
				e.Encode(args.Value)
				return s.Bytes()
			}(),},
			func(resp KVRPCResp) {
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply Config me: %d gid: %d %+v", kv.me, kv.gid, args)
				//fmt.Printf("reply Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			return true
		},

		func(leader int) {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Config me: %d gid: %d %+v", kv.me, kv.gid, args)
			//fmt.Printf("NotLeader Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply Config done me: %d gid: %d %+v", kv.me, kv.gid, issue.op)
	//fmt.Printf("reply Migrate done me: %d gid: %d %+v\n", kv.me, kv.gid, issue.op)
}

func (kv *ShardKV) updateShard(config shardmaster.Config, value map[string]string) {
	args := UpdateShardArgs{}
	args.ConfigNum = config.Num
	args.Value = value

	server := kv.me
	for {
		srv := kv.servers[server]
		var reply UpdateShardReply
		ok := false
		DPrintf("updateShard req to %d, %+v", server, args)
		fmt.Printf("updateShard req to %d, %+v\n", server, args)
		if server == kv.me {
			ok = true
			kv.UpdateShard(args, &reply)
		} else {
			ok = srv.Call("ShardKV.UpdateShard", &args, &reply)
		}
		DPrintf("Done updateShard req to %d, %+v", server, args)
		fmt.Printf("Done updateShard req to %d, %+v\n", server, args)
		if ok && reply.WrongLeader == false {
			return
		}
		server = reply.Leader
	}
	time.Sleep(100 * time.Millisecond)
}

func (kv *ShardKV) Config(args ConfigArgs, reply *ConfigReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{CONFIG, kv.me, -1, args.Config.Num,
			func() []byte {
				s := new(bytes.Buffer)
				e := labgob.NewEncoder(s)
				e.Encode(args.Config)
				return s.Bytes()
			}(),},
			func(resp KVRPCResp) {
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply Config me: %d gid: %d %+v", kv.me, kv.gid, args)
				//fmt.Printf("reply Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			return true
		},

		func(leader int) {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Config me: %d gid: %d %+v", kv.me, kv.gid, args)
			//fmt.Printf("NotLeader Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply Config done me: %d gid: %d %+v", kv.me, kv.gid, issue.op)
	//fmt.Printf("reply Migrate done me: %d gid: %d %+v\n", kv.me, kv.gid, issue.op)
}

func (kv *ShardKV) commitConfig(config shardmaster.Config) {
	args := ConfigArgs{}
	args.Config = config

	server := kv.me
	for {
		srv := kv.servers[server]
		var reply ConfigReply
		ok := false
		DPrintf("commitConfig req to %d, %+v", server, args)
		//fmt.Printf("commitConfig req to %d, %+v\n", server, args)
		if server == kv.me {
			ok = true
			kv.Config(args, &reply)
		} else {
			ok = srv.Call("ShardKV.Config", &args, &reply)
		}
		DPrintf("Done commitConfig req to %d, %+v", server, args)
		//fmt.Printf("Done commitConfig req to %d, %+v\n", server, args)
		if ok && reply.WrongLeader == false {
			return
		}
		server = reply.Leader
	}
	time.Sleep(100 * time.Millisecond)
}

func (kv *ShardKV) getShard(config shardmaster.Config, shard int, done chan struct{}) {
	args := GetShardArgs{}
	args.Shard = shard
	//fmt.Printf("GetShard %d req me %d, gid %d\n", shard, kv.me, kv.gid)
	kv.mu.Lock()
	if kv.config.Shards[shard] == kv.gid || config.Shards[shard] != kv.gid || kv.config.Num == 0{
		kv.mu.Unlock()
		done <- struct{}{}
		return
	}
	kv.mu.Unlock()

	for {
		args.ConfigNum = config.Num
		kv.mu.Lock()
		gid := kv.config.Shards[shard]
		//fmt.Printf("GetShard %d req to gid %d me %d, gid %d\n", shard, gid, kv.me, kv.gid)
		if servers, ok := kv.config.Groups[gid]; ok {
			kv.mu.Unlock()
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply GetShardReply
				DPrintf("GetShard req to %s, %+v", servers[si], args)
				//fmt.Printf("GetShard req to %s, %+v\n", servers[si], args)
				ok := srv.Call("ShardKV.GetShard", &args, &reply)
				DPrintf("Done GetShard req to %s, %+v", servers[si], args)
				//fmt.Printf("Done GetShard req to %s, %+v\n", servers[si], args)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					kv.updateShard(config, reply.Value)
					done <- struct{}{}
					return
				}
			}
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) issue(item KVRPCIssueItem) {
	if !item.preIssueCheck() {
		return
	}

	index, _, isLeader, leader := kv.rf.Start(*item.op)
	if !isLeader {
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
	kv.mu.Lock()
	kv.pendingIndex = index
	kv.mu.Unlock()
	kv.committing <- commit
	DPrintf("Waiting commitProcess me: %d gid: %d %+v", kv.me, kv.gid, item.op)
	<-commit.done
}

func (kv *ShardKV) issueProcess() {
	for {
		select {
		case item := <-kv.issueing:
			kv.issue(item)
			item.done <- struct{}{}
			DPrintf("issue done me: %d gid: %d %+v", kv.me, kv.gid, item.op)
			break
		case <-kv.ctx.Done():
			return
		}
	}
}

func (kv *ShardKV) execute(op *Op) (interface{}, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.OpCode {
	case PUT:
		key, value := "", ""
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&key); e != nil {
			panic(e)
		}
		if e := d.Decode(&value); e != nil {
			panic(e)
		}
		kv.DB[key] = value
		break
	case GET:
		key := ""
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&key); e != nil {
			panic(e)
		}
		v, exist := kv.DB[key]
		if !exist {
			return "", ErrNoKey
		}
		return v, OK
	case APPEND:
		key, value := "", ""
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&key); e != nil {
			panic(e)
		}
		if e := d.Decode(&value); e != nil {
			panic(e)
		}
		if v, exist := kv.DB[key]; !exist {
			kv.DB[key] = value
		} else {
			kv.DB[key] = v + value
		}
		break
	case GETSHARD:
		shard := 0
		value := make(map[string]string)
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&shard); e != nil {
			panic(e)
		}
		for k, v := range kv.DB {
			if key2shard(k) == shard {
				value[k] = v
			}
		}
		return value, OK
	case UPDATESHARD:
		value := make(map[string]string)
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&value); e != nil {
			panic(e)
		}
		for k, v := range value {
			kv.DB[k] = v
		}
		break
	case CONFIG:
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&kv.config); e != nil {
			panic(e)
		}
		break
	}
	return "", OK
}

func (kv *ShardKV) servePendingRPC(apply *raft.ApplyMsg, err Err, value interface{}) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if apply.CommandIndex == kv.pendingIndex {
		item := <-kv.committing
		op, ok := (apply.Command).(Op)
		DPrintf("commitProcess me: %d gid: %d %+v %+v Index:%d", kv.me, kv.gid, op, item.op, apply.CommandIndex)
		item.resp(KVRPCResp{
			op.SeqId != item.op.SeqId || op.ClerkId != item.op.ClerkId || !ok || !apply.CommandValid,
			op.ServerId,
			err,
			value,
		})
		close(item.done)
		return
	}
	if apply.CommandIndex > kv.pendingIndex {
		select {
		case item := <-kv.committing:
			DPrintf("commitProcess me: %d gid: %d  %+v Index:%d", kv.me, kv.gid, item.op, apply.CommandIndex)
			item.resp(KVRPCResp{
				true,
				kv.me,
				err,
				value,
			})
			close(item.done)

		default:

		}
	}

}

func (kv *ShardKV) updateClerkTrack(clerkId int64, seqId int) {
	kv.mu.Lock()
	kv.ClerkTrack[clerkId] = seqId
	v, ok := kv.ClerkTrack[clerkId]
	DPrintf("updateTrack me: %d gid: %d clerkId:%v seqId:%v ok:%v track:%v %v", kv.me, kv.gid, clerkId, seqId, ok, v, kv.ClerkTrack)
	kv.mu.Unlock()
}

func (kv *ShardKV) decodeSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&kv.DB); e != nil {
		panic(e)
	}
	if e := d.Decode(&kv.ClerkTrack); e != nil {
		panic(e)
	}
	if e := d.Decode(&kv.config); e != nil {
		panic(e)
	}
}

func (kv *ShardKV) encodeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	s := new(bytes.Buffer)
	e := labgob.NewEncoder(s)
	e.Encode(kv.DB)
	e.Encode(kv.ClerkTrack)
	e.Encode(kv.config)
	return s.Bytes()
}

func (kv *ShardKV) commitProcess() {
	for {
		select {
		case apply := <-kv.applyCh:
			var err Err
			var value interface{}
			if apply.CommandValid {
				op, _ := (apply.Command).(Op)
				value, err = kv.execute(&op)
				kv.updateClerkTrack(op.ClerkId, op.SeqId)
				DPrintf("server%d gid%d apply %+v Index:%d", kv.me, kv.gid, op, apply.CommandIndex)
			}
			if apply.Snapshot {
				snapshot, _ := (apply.Command).([]byte)
				DPrintf("install snapshot before decode me: %d gid: %d %+v", kv.me, kv.gid, kv.DB)
				kv.decodeSnapshot(snapshot)
			} else {
				kv.servePendingRPC(&apply, err, value)
				if apply.StageSize >= kv.maxraftstate && kv.maxraftstate > 0 {
					DPrintf("make snapshot me: %d gid: %d Index:%d stageSize %d %+v", kv.me, kv.gid, apply.CommandIndex, apply.StageSize, kv.DB)
					kv.rf.Snapshot(apply.CommandIndex, kv.encodeSnapshot())
				}
			}
			DPrintf("server%d gid%d apply Index:%d done", kv.me, kv.gid, apply.CommandIndex)
			break
		case <-kv.ctx.Done():
			return
		}
	}
}

func (kv *ShardKV) updateConfig() {
	if _, isLeader, _ := kv.rf.GetState(); !isLeader {
		return
	}
	config := kv.sm.Query(-1)
	kv.mu.Lock()
	if config.Num <= kv.config.Num {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	dones := make(chan struct{}, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i ++ {
		go kv.getShard(config, i, dones)
	}
	for i := 0; i < shardmaster.NShards; i ++ {
		<-dones
	}
	kv.commitConfig(config)
}

func (kv *ShardKV) pollConfig() {
	timer := time.NewTimer(100 * time.Millisecond)
	defer func() {
		timer.Stop()
	}()
	for {
		select {
		case <-timer.C:
			kv.updateConfig()
			timer.Reset(100 * time.Millisecond)
			break
		case <-kv.ctx.Done():
			return
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	DPrintf("server%d gid%d killed", kv.me, kv.gid)
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
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.ck = MakeClerk(masters, make_end)
	kv.sm = shardmaster.MakeClerk(masters)
	kv.servers = servers
	kv.booting = true
	kv.DB = make(map[string]string)
	kv.ClerkTrack = make(map[int64]int)
	kv.issueing = make(chan KVRPCIssueItem)
	kv.committing = make(chan KVRPCCommitItem, 1)
	kv.ctx, kv.cancel = context.WithCancel(context.Background())
	kv.pendingIndex = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, true)
	kv.decodeSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.commitProcess()
	go kv.issueProcess()
	go kv.pollConfig()
	DPrintf("server%d start", kv.me)
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	return kv
}
