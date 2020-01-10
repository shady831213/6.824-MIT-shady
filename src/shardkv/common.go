package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

const NOLEADER = "NoLeader"

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	SeqId   int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	Leader      int
	Server      int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	SeqId   int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	Leader      int
	Server      int
}

// Put or Append
type MigrateArgs struct {
	Value   map[string]string
	ClerkId int64
	SeqId   int
}

type MigrateReply struct {
	WrongLeader bool
	Err         Err
	Leader      int
	Server      int
}
