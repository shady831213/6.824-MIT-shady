package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.serverIds = make([]int, len(servers))
	for i := range ck.serverIds {
		ck.serverIds[i] = -1
	}
	ck.leaderIndex = 0
	ck.curSeqId = 0
	ck.id = nrand()
	return ck
}

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

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	// You will have to modify this function.

	var reply *QueryReply
	var ok bool
	req := func() {
		reply = &QueryReply{}
		args := QueryArgs{ArgsBase{ck.id, ck.curSeqId}, num}
		DPrintf("Query req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
		ok = ck.leadServer().Call("ShardMaster.Query", &args, reply)
		DPrintf("Done Query req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
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
	return reply.Config

}

func (ck *Clerk) Join(servers map[int][]string) {
	// You will have to modify this function.
	var reply *JoinReply
	var ok bool
	req := func() {
		reply = &JoinReply{}
		args := JoinArgs{ArgsBase{ck.id, ck.curSeqId}, servers}
		DPrintf("Join req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
		ok = ck.leadServer().Call("ShardMaster.Join", &args, reply)
		DPrintf("Join PutAppend req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
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

func (ck *Clerk) Leave(gids []int) {
	var reply *LeaveReply
	var ok bool
	req := func() {
		reply = &LeaveReply{}
		args := LeaveArgs{ArgsBase{ck.id, ck.curSeqId}, gids}
		DPrintf("Leave req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
		ok = ck.leadServer().Call("ShardMaster.Leave", &args, reply)
		DPrintf("Leave PutAppend req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
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

func (ck *Clerk) Move(shard int, gid int) {
	var reply *MoveReply
	var ok bool
	req := func() {
		reply = &MoveReply{}
		args := MoveArgs{ArgsBase{ck.id, ck.curSeqId}, shard, gid}
		DPrintf("Move req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
		ok = ck.leadServer().Call("ShardMaster.Move", &args, reply)
		DPrintf("Move PutAppend req to %d, %+v", ck.serverIds[ck.leaderIndex], args)
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
