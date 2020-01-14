package shardkv

import (
	"bytes"
	"context"
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
	STARTCONFIG = "StartConfig"
	ENDCONFIG   = "EndConfig"
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
	Config       shardmaster.Config
	NextConfig   shardmaster.Config
	MigrateDB    map[string]string
	booting      bool
	DB           map[string]string
	ClerkTrack   map[int64]int
	ShardTrack   [shardmaster.NShards]int
	ctx          context.Context
	cancel       func()
	issueing     chan KVRPCIssueItem
	committing   chan KVRPCCommitItem
	pendingIndex int
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
	if e := d.Decode(&kv.Config); e != nil {
		panic(e)
	}
	if e := d.Decode(&kv.ShardTrack); e != nil {
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
	e.Encode(kv.Config)
	e.Encode(kv.ShardTrack)
	return s.Bytes()
}

func (kv *ShardKV) servePendingRPC(apply *raft.ApplyMsg, err Err, value interface{}) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("servePendingRPC me: %d gid: %d pendingIdex:%d Index:%d", kv.me, kv.gid, kv.pendingIndex, apply.CommandIndex)
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
	defer kv.mu.Unlock()
	kv.ClerkTrack[clerkId] = seqId
	v, ok := kv.ClerkTrack[clerkId]
	DPrintf("updateTrack me: %d gid: %d clerkId:%v seqId:%v ok:%v track:%v %v", kv.me, kv.gid, clerkId, seqId, ok, v, kv.ClerkTrack)
}

func (kv *ShardKV) updateShadTrack(shard int, configNum int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ShardTrack[shard] = configNum
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

func (kv *ShardKV) checkShadTrack(shard int, configNum int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if configNum <= kv.ShardTrack[shard] {
		return false
	}
	return true
}

func (kv *ShardKV) checkGroup(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.Config.Groups[kv.gid]
	_, newOk := kv.NextConfig.Groups[kv.gid]
	shard := key2shard(key)
	return (kv.Config.Shards[shard] == kv.gid) && (kv.NextConfig.Shards[shard] == kv.gid) && ok && newOk

}

func (kv *ShardKV) curConfig() shardmaster.Config {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := kv.Config
	return config
}

func (kv *ShardKV) nextConfig() shardmaster.Config {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := kv.NextConfig
	return config
}

func (kv *ShardKV) updateCurConfig() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Config = kv.NextConfig
}

func (kv *ShardKV) updateNextConfig(config shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.NextConfig = config
}

func (kv *ShardKV) tryNextConfig() (shardmaster.Config, bool) {
	kv.mu.Lock()
	configNum := kv.Config.Num
	kv.mu.Unlock()
	config := kv.sm.Query(configNum + 1)
	return config, config.Num > configNum
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
		kvRPCItem{&Op{GETSHARD, kv.me, args.Gid, args.ConfigNum,
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
				DPrintf("reply GetShard me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
				//fmt.Printf("reply GetShard me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			if args.ConfigNum < kv.curConfig().Num {
				DPrintf("ignore GetShard me: %d gid: %d %+v", kv.me, kv.gid, args)
				return false
			}
			if args.ConfigNum != kv.nextConfig().Num {
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry GetShard me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
				//fmt.Printf("retry GetShard me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)
				return false
			}
			//fmt.Printf("get GetShard me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)
			return true
		},

		func(leader int) {
			reply.Server = kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader GetShard me: %d gid: %d %+v %+v", kv.me, kv.gid, args, reply)
			//fmt.Printf("NotLeader GetShard me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply GetShard done me: %d gid: %d %+v", kv.me, kv.gid, issue.op)
	//fmt.Printf("reply GetShard done me: %d gid: %d %+v\n", kv.me, kv.gid, issue.op)
}

func (kv *ShardKV) UpdateShard(args *UpdateShardArgs, reply *UpdateShardReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{UPDATESHARD, kv.me, -1, args.ConfigNum,
			func() []byte {
				s := new(bytes.Buffer)
				e := labgob.NewEncoder(s)
				e.Encode(args.Shard)
				e.Encode(args.ConfigNum)
				e.Encode(args.Value)
				return s.Bytes()
			}(),},
			func(resp KVRPCResp) {
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply UpdateShard me: %d gid: %d %+v", kv.me, kv.gid, args)
				////fmt.Printf("reply Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			if !kv.checkShadTrack(args.Shard, args.ConfigNum) {
				DPrintf("ignore UpdateShard me: %d gid: %d %+v", kv.me, kv.gid, args)
				return false
			}
			return true
		},

		func(leader int) {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader UpdateShard me: %d gid: %d %+v", kv.me, kv.gid, args)
			////fmt.Printf("NotLeader Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply UpdateShard done me: %d gid: %d %+v", kv.me, kv.gid, issue.op)
	////fmt.Printf("reply Migrate done me: %d gid: %d %+v\n", kv.me, kv.gid, issue.op)
}

func (kv *ShardKV) StartConfig(args *StartConfigArgs, reply *ConfigReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{STARTCONFIG, kv.me, -1, args.Config.Num,
			func() []byte {
				s := new(bytes.Buffer)
				e := labgob.NewEncoder(s)
				e.Encode(args.Config)
				return s.Bytes()
			}(),},
			func(resp KVRPCResp) {
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply StartConfig me: %d gid: %d %+v", kv.me, kv.gid, args)
				////fmt.Printf("reply Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			if args.Config.Num <= kv.curConfig().Num {
				DPrintf("ignore StartConfig me: %d gid: %d %+v", kv.me, kv.gid, args)
				return false
			}
			return true
		},

		func(leader int) {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader StartConfig me: %d gid: %d %+v", kv.me, kv.gid, args)
			////fmt.Printf("NotLeader Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply StartConfig done me: %d gid: %d %+v", kv.me, kv.gid, issue.op)
	////fmt.Printf("reply Migrate done me: %d gid: %d %+v\n", kv.me, kv.gid, issue.op)
}

func (kv *ShardKV) EndConfig(args *EndConfigArgs, reply *ConfigReply) {
	issue := KVRPCIssueItem{
		kvRPCItem{&Op{ENDCONFIG, kv.me, -1, args.ConfigNum,
			func() []byte { return []byte{} }(),},
			func(resp KVRPCResp) {
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply EndConfig me: %d gid: %d %+v", kv.me, kv.gid, args)
				////fmt.Printf("reply Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			if args.ConfigNum <= kv.curConfig().Num {
				DPrintf("ignore EndConfig me: %d gid: %d %+v", kv.me, kv.gid, args)
				return false
			}
			return true
		},

		func(leader int) {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader EndConfig me: %d gid: %d %+v", kv.me, kv.gid, args)
			////fmt.Printf("NotLeader Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply EndConfig done me: %d gid: %d %+v", kv.me, kv.gid, issue.op)
	////fmt.Printf("reply Migrate done me: %d gid: %d %+v\n", kv.me, kv.gid, issue.op)
}

func (kv *ShardKV) migrateReqs(args interface{}, name string, rpc func(*labrpc.ClientEnd) (bool, int)) {
	server := kv.me
	for {
		srv := kv.servers[server]
		DPrintf("%s req to %d gid: %d, %+v", name, server, kv.gid, args)
		//fmt.Printf("updateShard req to %d, %+v\n", server, args)
		ok, leader := rpc(srv)
		DPrintf("Done %s req to %d gid: %d, %+v", name, server, kv.gid, args)
		//fmt.Printf("Done updateShard req to %d, %+v\n", server, args)
		if ok {
			return
		}
		server = leader
	}
	time.Sleep(100 * time.Millisecond)
}

func (kv *ShardKV) updateShard(config shardmaster.Config, shard int, value map[string]string) {
	args := UpdateShardArgs{}
	args.ConfigNum = config.Num
	args.Shard = shard
	args.Value = value

	kv.migrateReqs(args, "UpdateShard", func(srv *labrpc.ClientEnd) (bool, int) {
		var reply UpdateShardReply
		ok := srv.Call("ShardKV.UpdateShard", &args, &reply)
		return ok && reply.WrongLeader == false, reply.Leader
	})
}

func (kv *ShardKV) startConfig(config shardmaster.Config) {
	args := StartConfigArgs{}
	args.Config = config

	kv.migrateReqs(args, "StartConfig", func(srv *labrpc.ClientEnd) (bool, int) {
		var reply ConfigReply
		ok := srv.Call("ShardKV.StartConfig", &args, &reply)
		return ok && reply.WrongLeader == false, reply.Leader
	})
}

func (kv *ShardKV) endConfig(configNum int) {
	args := EndConfigArgs{}
	args.ConfigNum = configNum

	kv.migrateReqs(args, "EndConfig", func(srv *labrpc.ClientEnd) (bool, int) {
		var reply ConfigReply
		ok := srv.Call("ShardKV.EndConfig", &args, &reply)
		return ok && reply.WrongLeader == false, reply.Leader
	})
}

func (kv *ShardKV) getShard(config shardmaster.Config, shard int, done chan struct{}) {
	args := GetShardArgs{}
	args.Shard = shard
	args.Gid = int64(kv.gid)
	defer func() { done <- struct{}{} }()
	curConfig := kv.curConfig()
	if curConfig.Shards[shard] == kv.gid || config.Shards[shard] != kv.gid || curConfig.Num == 0 {
		kv.updateShard(config, shard, make(map[string]string))
		return
	}

	for {
		args.ConfigNum = config.Num
		gid := curConfig.Shards[shard]
		if servers, ok := curConfig.Groups[gid]; ok {
			//fmt.Printf("GetShard %d req to gid %d me %d, gid %d\n", shard, gid, kv.me, kv.gid)
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply GetShardReply
				DPrintf("GetShard req to %s, %+v", servers[si], args)
				//fmt.Printf("GetShard req to %s, %+v\n", servers[si], args)
				ok := srv.Call("ShardKV.GetShard", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					DPrintf("Done GetShard req to %s, %+v", servers[si], args)
					//fmt.Printf("Done GetShard req to %s, %+v\n", servers[si], args)
					kv.updateShard(config, shard, reply.Value)
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) execute(op *Op) (interface{}, Err) {
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
		kv.updateClerkTrack(op.ClerkId, op.SeqId)
		break
	case GET:
		key := ""
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&key); e != nil {
			panic(e)
		}
		v, exist := kv.DB[key]
		kv.updateClerkTrack(op.ClerkId, op.SeqId)
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
		kv.updateClerkTrack(op.ClerkId, op.SeqId)
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
		//fmt.Printf("excute GetShard me: %d gid: %d %+v\n", kv.me, kv.gid, op)
		return value, OK
	case UPDATESHARD:
		shard := 0
		configNum := 0
		value := make(map[string]string)
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&shard); e != nil {
			panic(e)
		}
		if e := d.Decode(&configNum); e != nil {
			panic(e)
		}
		if e := d.Decode(&value); e != nil {
			panic(e)
		}
		for k, v := range value {
			kv.MigrateDB[k] = v
		}
		kv.updateShadTrack(shard, configNum)
		break
	case STARTCONFIG:
		config := shardmaster.Config{}
		r := bytes.NewBuffer(op.Value)
		d := labgob.NewDecoder(r)
		if e := d.Decode(&config); e != nil {
			panic(e)
		}
		kv.MigrateDB = make(map[string]string)
		kv.updateNextConfig(config)
		break
	case ENDCONFIG:
		for k, v := range kv.MigrateDB {
			kv.DB[k] = v
		}
		kv.updateCurConfig()
		break
	}
	return "", OK
}

func (kv *ShardKV) issue(item KVRPCIssueItem) {
	if !item.preIssueCheck() {
		return
	}
	kv.mu.Lock()
	index, _, isLeader, leader := kv.rf.Start(*item.op)
	if !isLeader {
		kv.mu.Unlock()
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

func (kv *ShardKV) commitProcess() {
	for {
		select {
		case apply := <-kv.applyCh:
			var err Err
			var value interface{}
			if apply.CommandValid {
				op, _ := (apply.Command).(Op)
				value, err = kv.execute(&op)
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
	config, update := kv.tryNextConfig()
	if !update {
		return
	}
	kv.startConfig(config)
	dones := make(chan struct{}, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i ++ {
		go kv.getShard(config, i, dones)
	}
	for i := 0; i < shardmaster.NShards; i ++ {
		<-dones
	}
	kv.endConfig(config.Num)
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
// StartConfig.Groups[gid][i] into a labrpc.ClientEnd on which you can
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
	kv.MigrateDB = make(map[string]string)
	kv.ClerkTrack = make(map[int64]int)
	kv.ShardTrack = [shardmaster.NShards]int{}
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
