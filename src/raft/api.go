package raft

type startReply struct {
	term   int
	role   RaftRole
	index  int
	leader int
}
type startReq struct {
	command interface{}
	reply   *startReply
	done    chan struct{}
}

func (rf *Raft) Start(command interface{}) (int, int, bool, int) {
	req := startReq{
		command,
		&startReply{
			-1,
			RaftFollower,
			-1,
			-1,
		},
		make(chan struct{}),
	}
	rf.startReqCh <- &req
	<-req.done
	return req.reply.index, req.reply.term, req.reply.role == RaftLeader, req.reply.leader
}

type snapshotReq struct {
	index int
	data  []byte
	done  chan struct{}
}

func (rf *Raft) Snapshot(index int, snapshotData []byte) {
	req := snapshotReq{
		index,
		snapshotData,
		make(chan struct{}),
	}
	rf.snapshotReqCh <- &req
	<-req.done
}

//called in restart
func (rf *Raft) GetSnapshot() *RaftSnapShot {
	return &rf.snapshot
}

// return currentTerm and whether this server
// believes it is the leader.
//Only for test, so not use msg :)
func (rf *Raft) GetState() (int, bool, int) {
	var term int
	var role RaftRole
	var leader int
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, role, leader = rf.currentTerm, rf.role, rf.leader
	return term, role == RaftLeader, leader
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type requestVoteReq struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
	done  chan struct{}
}

type requestVoteResp struct {
	args   *RequestVoteArgs
	reply  *RequestVoteReply
	server int
}

//
// example RequestVote RPC handler.
//

//the whole rpc must be atomic, then update state
//suppose rpc1 update term, then release lock, and rpc2 get lock update term again,
//rpc1 then get lock and vote, votefor will be not null, then rpc2 get lock again
//rpc2 has newer term but will not get vote, because votefor is not null now

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	RaftDebug("server", rf.me, "get request vote rpc from", args.CandidateId)
	req := requestVoteReq{
		args:  args,
		reply: reply,
		done:  make(chan struct{}),
	}
	rf.voteReqCh <- &req
	RaftDebug("server", rf.me, "waiting for vote rpc response to", args.CandidateId)
	<-req.done
	RaftDebug("server", rf.me, "response request vote rpc to", args.CandidateId)
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLogEntry
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
	//optimization
	ConflictIndex int
	ConflictTerm  int
}

type appendEntriesReq struct {
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
	done  chan struct{}
}

type appendEntriesResp struct {
	args   *AppendEntriesArgs
	reply  *AppendEntriesReply
	server int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	RaftDebug("server", rf.me, "get appendEntries rpc from", args.LeaderId)
	// Your code here (2A, 2B).
	req := appendEntriesReq{
		args:  args,
		reply: reply,
		done:  make(chan struct{}),
	}
	rf.appendEntriesReqCh <- &req
	<-req.done
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

type installSnapshotReq struct {
	args  *InstallSnapshotArgs
	reply *InstallSnapshotReply
	done  chan struct{}
}

type installSnapshotResp struct {
	args   *InstallSnapshotArgs
	reply  *InstallSnapshotReply
	server int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	RaftDebug("server", rf.me, "get installSnapshot rpc from", args.LeaderId)
	// Your code here (2A, 2B).
	req := installSnapshotReq{
		args:  args,
		reply: reply,
		done:  make(chan struct{}),
	}
	rf.installSnapshotReqCh <- &req
	<-req.done
}
