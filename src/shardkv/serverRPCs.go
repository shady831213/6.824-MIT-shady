package shardkv

import (
	"bytes"
	"labgob"
	"shardmaster"
)

type ShardKVRPC interface {
	op(interface{}, interface{}) KVRPCIssueItem
	execute(*Op) (interface{}, Err)
	init(*ShardKV)
}

type ShardKVRPCBase struct {
	kv *ShardKV
}

func (r *ShardKVRPCBase) init(kv *ShardKV) {
	r.kv = kv
}

func (r *ShardKVRPCBase) encodeArgs(args func(*labgob.LabEncoder)) []byte {
	s := new(bytes.Buffer)
	e := labgob.NewEncoder(s)
	args(e)
	return s.Bytes()
}

func (r *ShardKVRPCBase) decodeArgs(b []byte, args func(*labgob.LabDecoder)) {
	d := labgob.NewDecoder(bytes.NewBuffer(b))
	args(d)
}

type Get struct {
	ShardKVRPCBase
}

func (r *Get) op(ar interface{}, rp interface{}) KVRPCIssueItem {
	args := ar.(*GetArgs)
	reply := rp.(*GetReply)
	return KVRPCIssueItem{
		kvRPCItem{&Op{GET, r.kv.me, args.ClerkId, args.SeqId,
			r.encodeArgs(func(e *labgob.LabEncoder) {
				e.Encode(args.Key)
			}),},
			func(resp KVRPCResp) {
				reply.Server = r.kv.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				reply.Value = resp.value.(string)
				if value, ok := resp.value.(string); !ok {
					reply.Value = ""
				} else {
					reply.Value = value
				}
				DPrintf("reply Get me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
			},
			make(chan struct{})},

		func() bool {
			if !r.kv.checkGroup(args.Key) {
				reply.Err = ErrWrongGroup
				DPrintf("wrongGroup Get me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				return false
			}
			return true
		},
		func(leader int) {
			reply.Server = r.kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader Get me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
		},
	}
}

func (r *Get) execute(op *Op) (interface{}, Err) {
	key := ""
	r.decodeArgs(op.Value, func(d *labgob.LabDecoder) {
		if e := d.Decode(&key); e != nil {
			panic(e)
		}
	})
	if !r.kv.checkGroup(key) {
		DPrintf("wrongGroup %s me: %d gid: %d %+v", op.OpCode, r.kv.me, r.kv.gid, op)
		return "", ErrWrongGroup
	}
	v, exist := r.kv.DB[key]
	r.kv.updateClerkTrack(op.ClerkId, op.SeqId)
	if !exist {
		return "", ErrNoKey
	}
	return v, OK
}

type PugAppend struct {
	ShardKVRPCBase
}

func (r *PugAppend) op(ar interface{}, rp interface{}) KVRPCIssueItem {
	args := ar.(*PutAppendArgs)
	reply := rp.(*PutAppendReply)
	return KVRPCIssueItem{
		kvRPCItem{&Op{(OPCode)(args.Op), r.kv.me, args.ClerkId, args.SeqId,
			r.encodeArgs(func(e *labgob.LabEncoder) {
				e.Encode(args.Key)
				e.Encode(args.Value)
			}),},
			func(resp KVRPCResp) {
				reply.Server = r.kv.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply PutAppend me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
			},
			make(chan struct{})},

		func() bool {
			switch r.kv.checkClerkTrack(args.ClerkId, args.SeqId) {
			case ClerkIgnore:
				DPrintf("ignore PutAppend me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
				return false
			case ClerkRetry:
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry PutAppend me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
				return false
			}
			if !r.kv.checkGroup(args.Key) {
				reply.Err = ErrWrongGroup
				DPrintf("wrongGroup PutAppend me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				return false
			}
			return true
		},

		func(leader int) {
			reply.Server = r.kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader PutAppend me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
		},
	}
}

func (r *PugAppend) execute(op *Op) (interface{}, Err) {
	key, value := "", ""
	r.decodeArgs(op.Value, func(d *labgob.LabDecoder) {
		if e := d.Decode(&key); e != nil {
			panic(e)
		}
		if e := d.Decode(&value); e != nil {
			panic(e)
		}
	})
	if !r.kv.checkGroup(key) {
		DPrintf("wrongGroup %s me: %d gid: %d %+v", op.OpCode, r.kv.me, r.kv.gid, op)
		return "", ErrWrongGroup
	}
	switch op.OpCode {
	case PUT:
		r.kv.DB[key] = value
		r.kv.updateClerkTrack(op.ClerkId, op.SeqId)
		break
	case APPEND:
		if v, exist := r.kv.DB[key]; !exist {
			r.kv.DB[key] = value
		} else {
			r.kv.DB[key] = v + value
		}
		r.kv.updateClerkTrack(op.ClerkId, op.SeqId)
		break
	}
	return "", OK
}

type GetShard struct {
	ShardKVRPCBase
}

func (r *GetShard) op(ar interface{}, rp interface{}) KVRPCIssueItem {
	args := ar.(*GetShardArgs)
	reply := rp.(*GetShardReply)
	return KVRPCIssueItem{
		kvRPCItem{&Op{GETSHARD, r.kv.me, args.Gid, args.ConfigNum,
			r.encodeArgs(func(e *labgob.LabEncoder) {
				e.Encode(args.Shard)

			}),},
			func(resp KVRPCResp) {
				reply.Server = r.kv.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				reply.Value = resp.value.(map[string]string)
				DPrintf("reply GetShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
				//fmt.Printf("reply GetShard me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			if args.ConfigNum != r.kv.nextConfig().Num {
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry GetShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
				//fmt.Printf("retry GetShard me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)
				return false
			}
			//fmt.Printf("get GetShard me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)
			return true
		},

		func(leader int) {
			reply.Server = r.kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader GetShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
			//fmt.Printf("NotLeader GetShard me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
}

func (r *GetShard) execute(op *Op) (interface{}, Err) {
	shard := 0
	value := make(map[string]string)
	r.decodeArgs(op.Value, func(d *labgob.LabDecoder) {
		if e := d.Decode(&shard); e != nil {
			panic(e)
		}
	})

	for k, v := range r.kv.DB {
		if key2shard(k) == shard {
			value[k] = v
		}
	}
	r.kv.updateShadTrack(shard, op.SeqId)
	return value, OK
}

type UpdateShard struct {
	ShardKVRPCBase
}

func (r *UpdateShard) op(ar interface{}, rp interface{}) KVRPCIssueItem {
	args := ar.(*UpdateShardArgs)
	reply := rp.(*UpdateShardReply)
	return KVRPCIssueItem{
		kvRPCItem{&Op{UPDATESHARD, r.kv.me, -1, args.ConfigNum,
			r.encodeArgs(func(e *labgob.LabEncoder) {
				e.Encode(args.Shard)
				e.Encode(args.ConfigNum)
				e.Encode(args.Value)
			}),},
			func(resp KVRPCResp) {
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply UpdateShard me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				////fmt.Printf("reply Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			if !r.kv.checkShadTrack(args.Shard, args.ConfigNum) {
				DPrintf("ignore UpdateShard me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				return false
			}
			return true
		},

		func(leader int) {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader UpdateShard me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
			////fmt.Printf("NotLeader Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
}

func (r *UpdateShard) execute(op *Op) (interface{}, Err) {
	shard := 0
	configNum := 0
	value := make(map[string]string)
	r.decodeArgs(op.Value, func(d *labgob.LabDecoder) {
		if e := d.Decode(&shard); e != nil {
			panic(e)
		}
		if e := d.Decode(&configNum); e != nil {
			panic(e)
		}
		if e := d.Decode(&value); e != nil {
			panic(e)
		}
	})

	for k, v := range value {
		r.kv.DB[k] = v
	}
	r.kv.updateShadTrack(shard, configNum)
	return nil, OK
}

type StartConfig struct {
	ShardKVRPCBase
}

func (r *StartConfig) op(ar interface{}, rp interface{}) KVRPCIssueItem {
	args := ar.(*StartConfigArgs)
	reply := rp.(*ConfigReply)
	return KVRPCIssueItem{
		kvRPCItem{&Op{STARTCONFIG, r.kv.me, -1, args.Config.Num,
			r.encodeArgs(func(e *labgob.LabEncoder) {
				e.Encode(args.Config)
			}),},
			func(resp KVRPCResp) {
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply StartConfig me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				////fmt.Printf("reply Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			if args.Config.Num <= r.kv.curConfig().Num {
				DPrintf("ignore StartConfig me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				return false
			}
			return true
		},

		func(leader int) {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader StartConfig me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
			////fmt.Printf("NotLeader Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
}

func (r *StartConfig) execute(op *Op) (interface{}, Err) {
	config := shardmaster.Config{}
	r.decodeArgs(op.Value, func(d *labgob.LabDecoder) {
		if e := d.Decode(&config); e != nil {
			panic(e)
		}
	})
	r.kv.updateNextConfig(config)
	return nil, OK
}

type EndConfig struct {
	ShardKVRPCBase
}

func (r *EndConfig) op(ar interface{}, rp interface{}) KVRPCIssueItem {
	args := ar.(*EndConfigArgs)
	reply := rp.(*ConfigReply)
	return KVRPCIssueItem{
		kvRPCItem{&Op{ENDCONFIG, r.kv.me, -1, args.ConfigNum,
			[]byte{},},
			func(resp KVRPCResp) {
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply EndConfig me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				////fmt.Printf("reply Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			if args.ConfigNum <= r.kv.curConfig().Num {
				DPrintf("ignore EndConfig me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				return false
			}
			return true
		},

		func(leader int) {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader EndConfig me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
			////fmt.Printf("NotLeader Migrate me: %d gid: %d %+v %+v\n", kv.me, kv.gid, args, reply)

		},
	}
}

func (r *EndConfig) execute(op *Op) (interface{}, Err) {
	r.kv.updateCurConfig()
	return nil, OK
}
