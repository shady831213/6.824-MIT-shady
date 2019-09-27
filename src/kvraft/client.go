package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	curSeqId int
	id       int64
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
	ck.leader = 0
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
func (ck *Clerk) updateLeader() {
	if ck.leader == len(ck.servers)-1 {
		ck.leader = 0
	} else {
		ck.leader ++
	}
}

func (ck *Clerk) leadServer() *labrpc.ClientEnd {
	return ck.servers[ck.leader]
}

func (ck *Clerk) Get(key string) string {
	var reply *GetReply
	var ok bool
	req := func() {
		reply = &GetReply{}
		args := GetArgs{key, ck.id, ck.curSeqId}
		ok = ck.leadServer().Call("KVServer.Get", &args, reply)
	}
	for req(); !ok || reply.WrongLeader; req() {
		ck.updateLeader()
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
		ok = ck.leadServer().Call("KVServer.PutAppend", &args, reply)
	}
	for req(); !ok || reply.WrongLeader; req() {
		ck.updateLeader()
	}
	ck.curSeqId ++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
