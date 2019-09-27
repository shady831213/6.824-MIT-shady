package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

const ClerkTimeout = 1 * time.Second

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIndex int
	curSeqId    int
	id          int64
	serverIds   []int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.serverIds = make([]int, len(servers))
	for i := range ck.serverIds {
		ck.serverIds[i] = -1
	}
	ck.leaderIndex = 0
	ck.curSeqId = 0
	ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) nextLeaderIndex() {
	nextIndex := int(nrand() % int64((len(ck.servers))))
	for ;nextIndex == ck.leaderIndex; nextIndex = int(nrand() % int64((len(ck.servers)))){}
	ck.leaderIndex = nextIndex
}

func (ck *Clerk) getServerIndex(serverId int) int {
	for i, v := range ck.serverIds {
		if v == serverId {
			return i
		}
	}
	return -1
}

func (ck *Clerk) leadServer() *labrpc.ClientEnd {
	return ck.servers[ck.leaderIndex]
}

func (ck *Clerk) Get(key string) string {
	var reply *GetReply
	var ok bool
	req := func() {
		reply = &GetReply{}
		args := GetArgs{key, ck.id, ck.curSeqId}
		DPrintf("Get req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
		ok = ck.leadServer().Call("KVServer.Get", &args, reply)
		DPrintf("Done Get req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
		if ok && ck.getServerIndex(reply.Server) < 0 {
			ck.serverIds[ck.leaderIndex] = reply.Server
		}
	}
	for req(); !ok || reply.WrongLeader; req() {
		if !ok || reply.Leader < 0 {
			ck.nextLeaderIndex()
		} else if index := ck.getServerIndex(reply.Leader); index < 0 {
			ck.nextLeaderIndex()
		} else {
			ck.leaderIndex = index
		}
	}
	ck.curSeqId ++
	// You will have to modify this function.
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var reply *PutAppendReply
	var ok bool
	req := func() {
		reply = &PutAppendReply{}
		args := PutAppendArgs{key, value, op, ck.id, ck.curSeqId}
		DPrintf("PutAppend req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
		ok = ck.leadServer().Call("KVServer.PutAppend", &args, reply)
		DPrintf("Done PutAppend req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
		if ok && ck.getServerIndex(reply.Server) < 0 {
			ck.serverIds[ck.leaderIndex] = reply.Server
		}
	}
	for req(); !ok || reply.WrongLeader; req() {
		if !ok || reply.Leader < 0 {
			ck.nextLeaderIndex()
		} else if index := ck.getServerIndex(reply.Leader); index < 0 {
			ck.nextLeaderIndex()
		} else {
			ck.leaderIndex = index
		}
	}
	ck.curSeqId ++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
