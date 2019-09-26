package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader    int
	commitIdx int
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
	ck.commitIdx = 0
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
		args := GetArgs{key}
		ok = ck.leadServer().Call("KVServer.Get", &args, reply)
	}
	for req(); !ok || reply.WrongLeader; req() {
		ck.updateLeader()
	}
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
	req := func(retry bool) {
		reply = &PutAppendReply{}
		args := PutAppendArgs{key, value, op, retry, ck.commitIdx}
		ok = ck.leadServer().Call("KVServer.PutAppend", &args, reply)
	}
	for req(false); !ok || reply.WrongLeader; req(true) {
		ck.updateLeader()
	}
	if reply.CommitIdx > ck.commitIdx {
		ck.commitIdx = reply.CommitIdx
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
