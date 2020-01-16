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
	r.kv.updateClerkTrack(key2shard(key), op.ClerkId, op.SeqId)
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
			switch r.kv.checkClerkTrack(key2shard(args.Key), args.ClerkId, args.SeqId) {
			case ClerkIgnore:
				reply.Err = OK
				DPrintf("ignore PutAppend me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
				return false
			case ClerkRetry:
				reply.WrongLeader = true
				reply.Leader = -1
				DPrintf("retry PutAppend me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
				//fmt.Printf("retry PutAppend me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, reply)
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
		r.kv.updateClerkTrack(key2shard(key), op.ClerkId, op.SeqId)
		break
	case APPEND:
		if v, exist := r.kv.DB[key]; !exist {
			r.kv.DB[key] = value
		} else {
			r.kv.DB[key] = v + value
		}
		r.kv.updateClerkTrack(key2shard(key), op.ClerkId, op.SeqId)
		break
	}
	return "", OK
}

type GetShard struct {
	ShardKVRPCBase
}

type GetShardValue struct {
	Value map[string]string
	Track map[int64]int
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
				if value, ok := resp.value.(GetShardValue); !ok {
					reply.Value = make(map[string]string)
					reply.Track = make(map[int64]int)
				} else {
					reply.Value = value.Value
					reply.Track = value.Track
				}
				DPrintf("reply GetShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
				//fmt.Printf("reply GetShard me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			if args.ConfigNum > r.kv.nextConfig().Num {
				reply.Server = r.kv.me
				reply.WrongLeader = true
				DPrintf("retry GetShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, r.kv.nextConfig())
				//fmt.Printf("retry GetShard me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, r.kv.nextConfig())
				return false
			}
			track := r.kv.shadTrack(args.Shard)
			if args.ConfigNum == track.ConfigNum && track.State > ShardGet {
				reply.Err = ErrAlreadyDone
				DPrintf("ignore GetShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, track)
				//fmt.Printf("ignore GetShard me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, track)
				return false
			}

			return true
		},

		func(leader int) {
			reply.Server = r.kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader GetShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
			//fmt.Printf("NotLeader GetShard me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, reply)

		},
	}
}

func (r *GetShard) execute(op *Op) (interface{}, Err) {
	shard := 0
	value := GetShardValue{make(map[string]string), make(map[int64]int)}
	r.decodeArgs(op.Value, func(d *labgob.LabDecoder) {
		if e := d.Decode(&shard); e != nil {
			panic(e)
		}
	})

	for k, v := range r.kv.DB {
		if key2shard(k) == shard {
			value.Value[k] = v
		}
	}
	for k, v := range r.kv.ClerkTrack[shard] {
		value.Track[k] = v
	}

	r.kv.updateShadTrack(shard, ShardTrackItem{op.SeqId, ShardGet})
	return value, OK
}

type DeleteShard struct {
	ShardKVRPCBase
}

func (r *DeleteShard) op(ar interface{}, rp interface{}) KVRPCIssueItem {
	args := ar.(*DeleteShardArgs)
	reply := rp.(*DeleteShardReply)
	return KVRPCIssueItem{
		kvRPCItem{&Op{DELETESHARD, r.kv.me, args.Gid, args.ConfigNum,
			r.encodeArgs(func(e *labgob.LabEncoder) {
				e.Encode(args.Shard)

			}),},
			func(resp KVRPCResp) {
				reply.Server = r.kv.me
				reply.Err = resp.err
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply DeleteShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
				//fmt.Printf("reply DeleteShard me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, reply)

			},
			make(chan struct{})},

		func() bool {
			//fmt.Printf("check DeleteShard me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, r.kv.shadTrack(args.Shard))
			track := r.kv.shadTrack(args.Shard)
			if args.ConfigNum < track.ConfigNum || args.ConfigNum == track.ConfigNum && (track.State == ShardInit || track.State >= ShardDelete) {
				reply.Err = OK
				DPrintf("ignore DeleteShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, r.kv.shadTrack(args.Shard))
				//fmt.Printf("ignore DeleteShard me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, r.kv.shadTrack(args.Shard))
				return false
			}
			//if args.ConfigNum == track.ConfigNum && track.State < ShardGet || args.ConfigNum > r.kv.nextConfig().Num {
			//	reply.Server = r.kv.me
			//	reply.WrongLeader = true
			//	DPrintf("retry DeleteShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, r.kv.shadTrack(args.Shard))
			//	//fmt.Printf("retry DeleteShard me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, r.kv.shadTrack(args.Shard))
			//	return false
			//}
			return true
		},

		func(leader int) {
			reply.Server = r.kv.me
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader DeleteShard me: %d gid: %d %+v %+v", r.kv.me, r.kv.gid, args, reply)
			//fmt.Printf("NotLeader DeleteShard me: %d gid: %d %+v %+v\n", r.kv.me, r.kv.gid, args, reply)

		},
	}
}

func (r *DeleteShard) execute(op *Op) (interface{}, Err) {
	shard := 0
	value := GetShardValue{make(map[string]string), make(map[int64]int)}
	r.decodeArgs(op.Value, func(d *labgob.LabDecoder) {
		if e := d.Decode(&shard); e != nil {
			panic(e)
		}
	})

	for k, _ := range r.kv.DB {
		if key2shard(k) == shard {
			delete(r.kv.DB, k)
		}
	}
	r.kv.mu.Lock()
	r.kv.ClerkTrack[shard] = make(map[int64]int)
	r.kv.mu.Unlock()
	r.kv.updateShadTrack(shard, ShardTrackItem{op.SeqId, ShardDelete})
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
				e.Encode(args.Track)
			}),},
			func(resp KVRPCResp) {
				reply.WrongLeader = resp.wrongLeader
				reply.Leader = resp.leader
				DPrintf("reply UpdateShard me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				//fmt.Printf("reply UpdateShard me: %d gid: %d %+v\n", r.kv.me, r.kv.gid, args)

			},
			make(chan struct{})},

		func() bool {
			track := r.kv.shadTrack(args.Shard)
			if args.ConfigNum <= track.ConfigNum {
				DPrintf("ignore UpdateShard me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)
				return false
			}
			return true
		},

		func(leader int) {
			reply.WrongLeader = true
			reply.Leader = leader
			DPrintf("NotLeader UpdateShard me: %d gid: %d %+v", r.kv.me, r.kv.gid, args)

		},
	}
}

func (r *UpdateShard) execute(op *Op) (interface{}, Err) {
	shard := 0
	configNum := 0
	value := make(map[string]string)
	track := make(map[int64]int)

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
		if e := d.Decode(&track); e != nil {
			panic(e)
		}
	})

	for k, v := range value {
		r.kv.DB[k] = v
	}
	for k, v := range track {
		r.kv.updateClerkTrack(shard, k, v)
	}
	r.kv.updateShadTrack(shard, ShardTrackItem{configNum, ShardInit})
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
		},
	}
}

func (r *EndConfig) execute(op *Op) (interface{}, Err) {
	r.kv.updateCurConfig()
	return nil, OK
}
