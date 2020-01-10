package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"labrpc"
)
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	curSeqId map[int]int
	id       int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.curSeqId = make(map[int]int)
	ck.id = nrand()
	return ck
}

func (ck *Clerk) getGroup(key string) []string {
	shard := key2shard(key)
	gid := ck.config.Shards[shard]
	return ck.config.Groups[gid]
}

func (ck *Clerk) checkServer(key string, server string) bool {
	for _, v := range ck.getGroup(key) {
		if v == server {
			return true
		}
	}
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClerkId = ck.id
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.SeqId = ck.curSeqId[gid]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				DPrintf("Get req to %s, %+v", servers[si], args)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				DPrintf("Done Get req to %s, %+v", servers[si], args)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.curSeqId[gid] ++
					DPrintf("Success Get req to %s, %+v, config:%+v", servers[si], args, ck.config)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClerkId = ck.id

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.SeqId = ck.curSeqId[gid]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf("PutAppend req to %s, %+v", servers[si], args)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				DPrintf("Done PutAppend req to %s, %+v", servers[si], args)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					ck.curSeqId[gid] ++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

//func (ck *Clerk) Migrate(gid int, config shardmaster.Config, value map[string]string) {
//	args := MigrateArgs{}
//	args.Value = value
//	args.ClerkId = ck.id
//
//	for {
//		args.SeqId = ck.curSeqId[gid]
//		if servers, ok := config.Groups[gid]; ok {
//			for si := 0; si < len(servers); si++ {
//				srv := ck.make_end(servers[si])
//				var reply PutAppendReply
//				DPrintf("Migrate req to %s, %+v", servers[si], args)
//				fmt.Printf("Migrate req to %s, %+v\n", servers[si], args)
//				ok := srv.Call("ShardKV.Migrate", &args, &reply)
//				DPrintf("Done Migrate req to %s, %+v", servers[si], args)
//				fmt.Printf("Done Migrate req to %s, %+v\n", servers[si], args)
//				if ok && reply.WrongLeader == false && reply.Err == OK {
//					ck.curSeqId[gid] ++
//					return
//				}
//				if ok && reply.Err == ErrWrongGroup {
//					break
//				}
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}
