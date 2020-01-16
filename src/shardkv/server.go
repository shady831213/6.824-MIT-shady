package shardkv

import (
	"bytes"
	"context"
	"labgob"
	"labrpc"
	"raft"
	"shardmaster"
	"strconv"
	"sync"
	"time"
)

type OPCode string

const (
	GET         = "Get"
	PUT         = "Put"
	APPEND      = "Append"
	GETSHARD    = "GetShard"
	DELETESHARD = "DeleteShard"
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

type ShardState int

const (
	_ ShardState = iota
	ShardInit
	ShardGet
	ShardDelete
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

type ShardTrackItem struct {
	ConfigNum int
	State     ShardState
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
	rpcs         map[OPCode]ShardKVRPC
	servers      []*labrpc.ClientEnd
	ck           *Clerk
	sm           *shardmaster.Clerk
	Config       shardmaster.Config
	NextConfig   shardmaster.Config
	booting      bool
	DB           map[string]string
	ClerkTrack   [shardmaster.NShards]map[int64]int
	ShardTrack   [shardmaster.NShards]ShardTrackItem
	ctx          context.Context
	cancel       func()
	issueing     chan KVRPCIssueItem
	committing   chan KVRPCCommitItem
	pendingIndex int
}

func (kv *ShardKV) registerRPC(key OPCode, rpc ShardKVRPC) {
	rpc.init(kv)
	kv.rpcs[key] = rpc
}

func (kv *ShardKV) issueRPC(key OPCode, args interface{}, reply interface{}) {
	issue := kv.rpcs[key].op(args, reply)
	kv.issueing <- issue
	<-issue.done
	DPrintf("reply %s done me: %d gid: %d %+v", key, kv.me, kv.gid, issue.op)
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
	config := shardmaster.Config{}
	if e := d.Decode(&config); e != nil {
		panic(e)
	}
	kv.Config = config
	nConfig := shardmaster.Config{}
	if e := d.Decode(&nConfig); e != nil {
		panic(e)
	}
	kv.NextConfig = nConfig
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
	e.Encode(kv.NextConfig)
	e.Encode(kv.ShardTrack)
	return s.Bytes()
}

func (kv *ShardKV) updateBooting(term int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.booting {
		t, _, _ := kv.rf.GetState()
		if term >= t {
			kv.booting = false
		}
	}
}

func (kv *ShardKV) isKilled() bool {
	select {
	case <-kv.ctx.Done():
		return true
	default:
		return false
	}
}

func (kv *ShardKV) updateClerkTrack(shard int, clerkId int64, seqId int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ClerkTrack[shard][clerkId] = seqId
	v, ok := kv.ClerkTrack[shard][clerkId]
	DPrintf("updateTrack me: %d gid: %d clerkId:%v shard:%d seqId:%v ok:%v track:%v %v", kv.me, kv.gid, clerkId, shard, seqId, ok, v, kv.ClerkTrack)
}

func (kv *ShardKV) updateShadTrack(shard int, item ShardTrackItem) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ShardTrack[shard] = item
}

func (kv *ShardKV) checkClerkTrack(shard int, clerkId int64, sedId int) ClerkTrackAction {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.ClerkTrack[shard][clerkId]
	DPrintf("checkClerkTrack me: %d gid: %d clerkId:%v shard:%d ok:%v seqId:%d, v:%d, %v", kv.me, kv.gid, clerkId, shard, ok, sedId, v, kv.ClerkTrack)
	if !ok && sedId > 0 || sedId > v+1 || kv.booting {
		//fmt.Printf("checkClerkTrack me: %d gid: %d clerkId:%v shard:%d ok:%v seqId:%d, v:%d, %v\n", kv.me, kv.gid, clerkId, shard, ok, sedId, v, kv.ClerkTrack)
		return ClerkRetry
	}
	if !ok && sedId == 0 || sedId == v+1 {
		return ClerkOK
	}
	return ClerkIgnore
}

func (kv *ShardKV) shadTrack(shard int) ShardTrackItem {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	i := kv.ShardTrack[shard]
	return i
}

func (kv *ShardKV) checkGroup(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, existGroup := kv.Config.Groups[kv.gid]
	_, newExistGroup := kv.NextConfig.Groups[kv.gid]
	shard := key2shard(key)
	DPrintf("checkGroup me: %d gid: %d config:%+v, nextconfig:%+v, track:%+v", kv.me, kv.gid, kv.Config, kv.NextConfig, kv.ShardTrack)
	ok := (kv.Config.Shards[shard] == kv.gid) && (kv.NextConfig.Shards[shard] == kv.gid) && existGroup && newExistGroup && (kv.Config.Num == kv.NextConfig.Num)
	return ok

}

func (kv *ShardKV) curConfig() shardmaster.Config {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := shardmaster.Config{}
	config.Num = kv.Config.Num
	for k, v := range kv.Config.Shards {
		config.Shards[k] = v
	}
	config.Groups = make(map[int][]string)
	for k, v := range kv.Config.Groups {
		config.Groups[k] = v
	}
	return config
}

func (kv *ShardKV) nextConfig() shardmaster.Config {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := shardmaster.Config{}
	config.Num = kv.NextConfig.Num
	for k, v := range kv.NextConfig.Shards {
		config.Shards[k] = v
	}
	config.Groups = make(map[int][]string)
	for k, v := range kv.NextConfig.Groups {
		config.Groups[k] = v
	}
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
	defer kv.mu.Unlock()
	config := kv.sm.Query(kv.Config.Num + 1)
	return config, config.Num > kv.Config.Num && !kv.booting
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.issueRPC(GET, args, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.issueRPC(OPCode(args.Op), args, reply)
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	kv.issueRPC(GETSHARD, args, reply)

}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	kv.issueRPC(DELETESHARD, args, reply)

}

func (kv *ShardKV) UpdateShard(args *UpdateShardArgs, reply *UpdateShardReply) {
	kv.issueRPC(UPDATESHARD, args, reply)

}

func (kv *ShardKV) StartConfig(args *StartConfigArgs, reply *ConfigReply) {
	kv.issueRPC(STARTCONFIG, args, reply)

}

func (kv *ShardKV) EndConfig(args *EndConfigArgs, reply *ConfigReply) {
	kv.issueRPC(ENDCONFIG, args, reply)

}

func (kv *ShardKV) migrateReqs(args interface{}, name string, rpc func(int) (bool, int)) {
	server := kv.me
	for {
		DPrintf("%s req to %d gid: %d, %+v", name, server, kv.gid, args)
		ok, leader := rpc(server)
		DPrintf("Done %s req to %d gid: %d, %+v", name, server, kv.gid, args)
		if ok {
			return
		}
		if kv.isKilled() {
			return
		}
		if leader < 0 {
			server = kv.me
		} else {
			server = leader
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) updateShard(config shardmaster.Config, shard int, value map[string]string, track map[int64]int) {
	args := UpdateShardArgs{}
	args.ConfigNum = config.Num
	args.Shard = shard
	args.Value = value
	args.Track = track

	kv.migrateReqs(args, "UpdateShard", func(server int) (bool, int) {
		var reply UpdateShardReply
		if server == kv.me {
			kv.UpdateShard(&args, &reply)
			return reply.WrongLeader == false, reply.Leader
		}
		srv := kv.servers[server]
		ok := srv.Call("ShardKV.UpdateShard", &args, &reply)
		return ok && reply.WrongLeader == false, reply.Leader
	})
}

func (kv *ShardKV) startConfig(config shardmaster.Config) {
	args := StartConfigArgs{}
	args.Config = config

	kv.migrateReqs(args, "StartConfig", func(server int) (bool, int) {
		var reply ConfigReply
		if server == kv.me {
			kv.StartConfig(&args, &reply)
			return reply.WrongLeader == false, reply.Leader
		}
		srv := kv.servers[server]
		ok := srv.Call("ShardKV.StartConfig", &args, &reply)
		return ok && reply.WrongLeader == false, reply.Leader
	})
}

func (kv *ShardKV) endConfig(configNum int) {
	args := EndConfigArgs{}
	args.ConfigNum = configNum

	kv.migrateReqs(args, "EndConfig", func(server int) (bool, int) {
		var reply ConfigReply
		if server == kv.me {
			kv.EndConfig(&args, &reply)
			return reply.WrongLeader == false, reply.Leader
		}
		srv := kv.servers[server]
		ok := srv.Call("ShardKV.EndConfig", &args, &reply)
		return ok && reply.WrongLeader == false, reply.Leader
	})
}

func (kv *ShardKV) deleteShard(config shardmaster.Config, shard int) {
	args := DeleteShardArgs{}
	args.Shard = shard
	args.Gid = int64(kv.gid)
	args.ConfigNum = config.Num
	curConfig := kv.curConfig()
	for {
		args.ConfigNum = config.Num
		gid := curConfig.Shards[shard]
		if servers, ok := curConfig.Groups[gid]; ok {
			DPrintf("DeleteShard %d req to gid %d me %d, gid %d, %+v", shard, gid, kv.me, kv.gid, curConfig)
			//fmt.Printf("DeleteShard %d req to gid %d me %d, gid %d, %+v\n", shard, gid, kv.me, kv.gid, curConfig)

			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply DeleteShardReply
				DPrintf("DeleteShard req to %s, %+v", servers[si], args)
				//fmt.Printf("DeleteShard req to %s, %+v\n", servers[si], args)

				ok := srv.Call("ShardKV.DeleteShard", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					DPrintf("Done DeleteShard req to %s, %+v", servers[si], args)
					//fmt.Printf("Done DeleteShard req to %s, %+v %+v\n", servers[si], args, reply)
					return
				}
				if kv.isKilled() {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) getShard(config shardmaster.Config, shard int, done chan struct{}) {
	args := GetShardArgs{}
	args.Shard = shard
	args.Gid = int64(kv.gid)
	defer func() { done <- struct{}{} }()
	curConfig := kv.curConfig()
	if curConfig.Shards[shard] == kv.gid && config.Shards[shard] == kv.gid ||
		curConfig.Shards[shard] != kv.gid && config.Shards[shard] != kv.gid ||
		curConfig.Num == 0 {
		kv.updateShard(config, shard, make(map[string]string), make(map[int64]int))
		return
	}

	if curConfig.Shards[shard] == kv.gid && config.Shards[shard] != kv.gid {
		for track := kv.shadTrack(shard); config.Num != track.ConfigNum || track.State != ShardDelete; track = kv.shadTrack(shard) {
			if kv.isKilled() {
				return
			}
		}
		kv.updateShard(config, shard, make(map[string]string), make(map[int64]int))
		return
	}

	for {
		args.ConfigNum = config.Num
		gid := curConfig.Shards[shard]
		if servers, ok := curConfig.Groups[gid]; ok {
			DPrintf("GetShard %d req to gid %d me %d, gid %d, %+v", shard, gid, kv.me, kv.gid, curConfig)
			//fmt.Printf("GetShard %d req to gid %d me %d, gid %d, %+v\n", shard, gid, kv.me, kv.gid, curConfig)

			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply GetShardReply
				DPrintf("GetShard req to %s, %+v", servers[si], args)
				//fmt.Printf("GetShard req to %s, %+v\n", servers[si], args)

				ok := srv.Call("ShardKV.GetShard", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					DPrintf("Done GetShard req to %s, %+v", servers[si], args)
					//fmt.Printf("Done GetShard req to %s, %+v %+v\n", servers[si], args, reply)

					kv.deleteShard(config, shard)
					kv.updateShard(config, shard, reply.Value, reply.Track)
					return
				}
				if kv.isKilled() || ok && reply.WrongLeader == false && reply.Err == ErrAlreadyDone {
					DPrintf("Done GetShard req to %s, %+v", servers[si], args)
					//fmt.Printf("Ignore GetShard req to %s, %+v %+v\n", servers[si], args, reply)
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) execute(op *Op) (interface{}, Err) {
	return kv.rpcs[op.OpCode].execute(op)
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
				DPrintf("install snapshot before decode me: %d gid: %d %+v config:%+v", kv.me, kv.gid, kv.DB, kv.curConfig())
				kv.decodeSnapshot(snapshot)
				DPrintf("install snapshot after decode me: %d gid: %d %+v config:%+v", kv.me, kv.gid, kv.DB, kv.curConfig())
			} else {
				kv.servePendingRPC(&apply, err, value)
				if apply.StageSize >= kv.maxraftstate && kv.maxraftstate > 0 {
					DPrintf("make snapshot me: %d gid: %d Index:%d stageSize %d %+v", kv.me, kv.gid, apply.CommandIndex, apply.StageSize, kv.DB)

					kv.rf.Snapshot(apply.CommandIndex, kv.encodeSnapshot())
				}
			}
			DPrintf("server%d gid%d apply Index:%d done", kv.me, kv.gid, apply.CommandIndex)
			kv.updateBooting(apply.CommandTerm)
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
	DPrintf("begin startConfig me: %d gid: %d cur:%+v, next:%+v", kv.me, kv.gid, kv.curConfig(), config)
	//fmt.Printf("begin startConfig me: %d gid: %d cur:%+v, next:%+v\n", kv.me, kv.gid, kv.curConfig(), config)

	kv.startConfig(config)
	DPrintf("begin updateShark me: %d gid: %d cur:%+v, next:%+v", kv.me, kv.gid, kv.curConfig(), config)
	//fmt.Printf("begin updateShark me: %d gid: %d cur:%+v, next:%+v\n", kv.me, kv.gid, kv.curConfig(), config)

	dones := make(chan struct{}, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i ++ {
		go kv.getShard(config, i, dones)
	}
	for i := 0; i < shardmaster.NShards; i ++ {
		<-dones
		DPrintf("end 1 updateShark me: %d gid: %d cur:%+v, next:%+v", kv.me, kv.gid, kv.curConfig(), config)
		//fmt.Printf("end 1 updateShark me: %d gid: %d cur:%+v, next:%+v\n", kv.me, kv.gid, kv.curConfig(), config)

	}
	DPrintf("begin endConfig me: %d gid: %d cur:%+v, next:%+v", kv.me, kv.gid, kv.curConfig(), config)
	//fmt.Printf("begin endConfig me: %d gid: %d cur:%+v, next:%+v\n", kv.me, kv.gid, kv.curConfig(), config)

	kv.endConfig(config.Num)
	DPrintf("end endConfig me: %d gid: %d cur:%+v, next:%+v", kv.me, kv.gid, kv.curConfig(), config)
	//fmt.Printf("end endConfig me: %d gid: %d cur:%+v, next:%+v\n", kv.me, kv.gid, kv.curConfig(), config)

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
	DPrintf("server%d gid%d killed", kv.me, kv.gid)

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
	labgob.Register(shardmaster.Config{})
	labgob.Register(ShardTrackItem{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.rpcs = make(map[OPCode]ShardKVRPC)
	kv.registerRPC(GET, new(Get))
	kv.registerRPC(PUT, new(PugAppend))
	kv.registerRPC(APPEND, new(PugAppend))
	kv.registerRPC(GETSHARD, new(GetShard))
	kv.registerRPC(DELETESHARD, new(DeleteShard))
	kv.registerRPC(UPDATESHARD, new(UpdateShard))
	kv.registerRPC(STARTCONFIG, new(StartConfig))
	kv.registerRPC(ENDCONFIG, new(EndConfig))
	kv.ck = MakeClerk(masters, make_end)
	kv.sm = shardmaster.MakeClerk(masters)
	kv.servers = servers
	kv.booting = true
	kv.Config = shardmaster.Config{}
	kv.NextConfig = shardmaster.Config{}
	kv.DB = make(map[string]string)
	kv.ClerkTrack = [shardmaster.NShards]map[int64]int{}
	for s := range kv.ClerkTrack {
		kv.ClerkTrack[s] = make(map[int64]int)
	}
	kv.ShardTrack = [shardmaster.NShards]ShardTrackItem{}
	kv.issueing = make(chan KVRPCIssueItem)
	kv.committing = make(chan KVRPCCommitItem, 1)
	kv.ctx, kv.cancel = context.WithCancel(context.Background())
	kv.pendingIndex = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, true)
	kv.rf.Tag = strconv.Itoa(gid)
	// You may need initialization code here.
	go kv.commitProcess()
	go kv.issueProcess()
	go kv.pollConfig()
	DPrintf("server%d gid%d started", kv.me, kv.gid)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	return kv
}
