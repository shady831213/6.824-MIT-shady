package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
var raftDebug bool

type RaftRole int

const (
	_ RaftRole = iota
	RaftFollower
	RaftCandidate
	RaftLeader
	RaftStop
)

const RaftHeartBeatPeriod = 50 * time.Millisecond

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftLogEntry struct {
	Command interface{}
	Term    int
}

type RaftSnapShot struct {
	Index int
	Term  int
	Data  []byte
}

const DummyRaftCommand = "DummyRaftCommand"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	debug      bool
	dummyCmdEn bool
	//role state
	leader              int
	role                RaftRole
	ctx                 context.Context
	cancel              func()
	voteReqCh           chan *requestVoteReq
	appendEntriesReqCh  chan *appendEntriesReq
	voteRespCh          chan *requestVoteResp
	appendEntriesRespCh chan *appendEntriesResp
	// persistent states
	currentTerm int
	votedFor    int
	logs        []RaftLogEntry
	snapshot    RaftSnapShot
	// volatile states
	commitIndex int
	lastApplied int

	// volatile states for leader
	nextIndex    []int
	matchedIndex []int
}

func (rf *Raft) logPosition(i int) int {
	if i < rf.snapshot.Index {
		panic(fmt.Sprint(i, "< snapshot index", rf.snapshot.Index))
	}
	return i - rf.snapshot.Index
}

func (rf *Raft) logIndex(i int) int {
	return i + rf.snapshot.Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool, int) {
	var term int
	var role RaftRole
	var leader int
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, role, leader = rf.currentTerm, rf.role, rf.leader
	return term, role == RaftLeader, leader
}

func (rf *Raft) apply(applyChan chan ApplyMsg) {
	entries := make([]RaftLogEntry, 0)
	lastApplied := rf.lastApplied + 1
	rf.mu.Lock()
	//println("server", rf.me, "apply, lastAppliy", rf.lastApplied, "commitIndex", rf.commitIndex, fmt.Sprintf("logs %+v", rf.logs))
	if rf.lastApplied < rf.commitIndex {
		entries = append(entries, rf.logs[rf.logPosition(rf.lastApplied+1):rf.logPosition(rf.commitIndex+1)]...)
		//println("server", rf.me, "apply log lastAppliy", rf.lastApplied, "commitIndex", rf.commitIndex, fmt.Sprintf("logs %+v entries %+v", rf.logs, entries))
		//println()
	}
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()
	for i, entry := range entries {
		//RaftDebug("server", rf.me, "applyIndex", rf.lastApplied, "commitIndex", rf.commitIndex, "log", rf.logs)
		//RaftDebug("server", rf.me, "apply", ApplyMsg{true, command, rf.lastApplied})
		applyChan <- ApplyMsg{entry.Command != DummyRaftCommand, entry.Command, lastApplied + i}
	}
}

//must be inside critical region
func (rf *Raft) lastLogEntryInfo() (int, int) {
	lastIndex := len(rf.logs) - 1
	lastTerm := rf.logs[lastIndex].Term
	return rf.logIndex(lastIndex), lastTerm
}

//must be inside critical region
func (rf *Raft) lastFollowerEntryInfo(follower int) (int, int, int) {
	index := rf.nextIndex[follower] - 1
	RaftDebug("server", rf.me, "matchedIndex of follower", follower, rf.logPosition(index), rf.nextIndex[follower])
	return rf.logPosition(index), rf.logs[rf.logPosition(index)].Term, rf.commitIndex
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	state := new(bytes.Buffer)
	stateE := labgob.NewEncoder(state)
	stateE.Encode(rf.currentTerm)
	stateE.Encode(rf.votedFor)
	stateE.Encode(rf.logs)

	snapshot := new(bytes.Buffer)
	snapshotE := labgob.NewEncoder(snapshot)
	snapshotE.Encode(rf.snapshot.Index)
	snapshotE.Encode(rf.snapshot.Term)
	snapshotE.Encode(rf.snapshot.Data)
	rf.persister.SaveStateAndSnapshot(state.Bytes(), snapshot.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&rf.currentTerm); e != nil {
		panic(e)
	}
	if e := d.Decode(&rf.votedFor); e != nil {
		panic(e)
	}
	if e := d.Decode(&rf.logs); e != nil {
		panic(e)
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&rf.snapshot.Index); e != nil {
		panic(e)
	}
	if e := d.Decode(&rf.snapshot.Term); e != nil {
		panic(e)
	}
	if e := d.Decode(&rf.snapshot.Data); e != nil {
		panic(e)
	}
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

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote() {
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				lastIndex, lastTerm := rf.lastLogEntryInfo()
				term, role := rf.currentTerm, rf.role
				rf.mu.Unlock()
				if role == RaftCandidate {
					args := RequestVoteArgs{
						term,
						rf.me,
						lastIndex,
						lastTerm}
					reply := RequestVoteReply{}
					RaftDebug("server", rf.me, "send request vote to", server)
					if ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply); ok {
						//deal response
						RaftDebug("server", rf.me, "get request vote response from", server)
						rf.voteRespCh <- &requestVoteResp{args: &args, reply: &reply, server: server}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) sendOneAppendEntries(server int) bool {
	var entries []RaftLogEntry
	rf.mu.Lock()
	term, role := rf.currentTerm, rf.role
	lastIndex, lastTerm, commitIndex := rf.lastFollowerEntryInfo(server)
	if rf.logIndex(len(rf.logs)-1) > lastIndex {
		entries = append(entries, rf.logs[rf.logPosition(lastIndex+1):]...)
	}
	rf.mu.Unlock()
	RaftDebug("server", rf.me, "before send appendEntries to", server, "role", role)
	if role == RaftLeader {
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: lastIndex,
			PrevLogTerm:  lastTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}
		reply := AppendEntriesReply{}
		RaftDebug("server", rf.me, "send appendEntries to", server)
		if ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply); ok {
			//deal response
			RaftDebug("server", rf.me, "get appendEntries response from", server)
			rf.appendEntriesRespCh <- &appendEntriesResp{args: &args, reply: &reply, server: server}

		}
	}
	return true
}

func (rf *Raft) sendAppendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendOneAppendEntries(i)
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) start(entry RaftLogEntry) int {
	var index int

	index = rf.logIndex(len(rf.logs))
	rf.logs = append(rf.logs, entry)
	rf.persist()
	RaftDebug("server", rf.me, "start cmd", entry.Command, "logs", rf.logs)
	//println("server", rf.me, "start cmd", entry.Command, "logs", rf.logs)
	rf.sendAppendEntries()
	return index
}

func (rf *Raft) Start(command interface{}) (int, int, bool, int) {
	index := -1
	term := -1
	isLeader := true
	leader := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if term, isLeader, leader = rf.currentTerm, rf.role == RaftLeader, rf.leader; !isLeader {
		return index, term, isLeader, leader
	}
	index = rf.start(RaftLogEntry{command, term})

	return index, term, isLeader, rf.me
}

func (rf *Raft) Snapshot(index int, snapshotData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshot.Index = index
	rf.snapshot.Term = rf.logs[index].Term
	rf.snapshot.Data = append(rf.snapshot.Data, snapshotData...)
	rf.logs = rf.logs[rf.logPosition(index+1):]
	rf.persist()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	RaftDebug("server", rf.me, "shutdown!")
	rf.cancel()
}

//
// Raft state machine
//

func (rf *Raft) requestVote(req *requestVoteReq) {
	defer func() {
		req.done <- struct{}{}
		close(req.done)
	}()
	lastIndex, lastTerm := rf.lastLogEntryInfo()
	req.reply.Term = rf.currentTerm
	rf.leader = -1
	RaftDebug("server", rf.me, "term", rf.currentTerm, "request term", req.args.Term)
	if req.args.Term < rf.currentTerm {
		req.reply.VoteGranted = false
		return
	}
	RaftDebug("server", rf.me, "votedFor", rf.votedFor, "request id", req.args.CandidateId)
	if rf.votedFor >= 0 && rf.votedFor != req.args.CandidateId {
		req.reply.VoteGranted = false
		return
	}

	RaftDebug("server", rf.me, "LastLogTerm", lastTerm, "request LastLogTerm", req.args.LastLogTerm)
	if req.args.LastLogTerm < lastTerm {
		req.reply.VoteGranted = false
		return
	}
	RaftDebug("server", rf.me, "LastLogIndex", lastIndex, "request LastLogIndex", req.args.LastLogIndex)
	if req.args.LastLogTerm == lastTerm && req.args.LastLogIndex < lastIndex {
		req.reply.VoteGranted = false
		return
	}

	req.reply.VoteGranted = true
	rf.votedFor = req.args.CandidateId
	rf.persist()
	RaftDebug("server", rf.me, "vote to", req.args.CandidateId)
}

func (rf *Raft) appendEntries(req *appendEntriesReq) {
	defer func() {
		req.done <- struct{}{}
		close(req.done)
	}()
	RaftDebug("server", rf.me, "term", rf.currentTerm, "request term", req.args.Term, "server role", rf.role)
	req.reply.Term = rf.currentTerm
	if req.args.Term < rf.currentTerm {
		req.reply.Success = false
		return
	}
	RaftDebug("server", rf.me, "get appendEntries rpc from", req.args.LeaderId, "PrevLogIndex", req.args.PrevLogIndex, "logs", rf.logs, "entries", req.args.Entries)
	//not exist
	if req.args.PrevLogIndex > rf.logIndex(len(rf.logs)-1) {
		req.reply.Success = false
		req.reply.ConflictIndex = rf.logIndex(len(rf.logs))
		req.reply.ConflictTerm = -1
		return
	}
	//term not match
	if entry := rf.logs[rf.logPosition(req.args.PrevLogIndex)]; entry.Term != req.args.PrevLogTerm {
		req.reply.Success = false
		req.reply.ConflictTerm = entry.Term
		for i := req.args.PrevLogIndex; i >= 0; i -- {
			if rf.logs[rf.logPosition(i)].Term != req.reply.ConflictTerm {
				req.reply.ConflictIndex = i + 1
				break
			}
		}
		return
	}
	//check conflict
	RaftDebug("server", rf.me, "get appendEntries rpc from", req.args.LeaderId, "can update entries")
	if req.args.PrevLogIndex < rf.logIndex(len(rf.logs)-1) {
		for i, e := range req.args.Entries {
			if rf.logs[rf.logPosition(req.args.PrevLogIndex+1+i)].Term != e.Term || rf.logPosition(req.args.PrevLogIndex+1+i) == len(rf.logs)-1 {
				rf.logs = append(rf.logs[:rf.logPosition(req.args.PrevLogIndex+1+i)], req.args.Entries[i:]...)
				break
			}
		}
	} else {
		//rf.logs all committed
		rf.logs = append(rf.logs, req.args.Entries...)
	}

	rf.persist()
	//append new entries
	RaftDebug("server", rf.me, "get appendEntries rpc from", req.args.LeaderId, "logs", rf.logs, "entries", req.args.Entries)

	//update commitIndex
	if req.args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = rf.logIndex(len(rf.logs) - 1)
		if req.args.LeaderCommit < rf.commitIndex {
			rf.commitIndex = req.args.LeaderCommit
		}
	}
	//println("server", rf.me, "update commitIndex as follower", rf.commitIndex, "log len =", len(rf.logs))
	//fmt.Printf("logs %+v\n", rf.logs)
	//println()
	rf.leader = req.args.LeaderId
	req.reply.Success = true

}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Int()%10+2) * RaftHeartBeatPeriod
}

func (rf *Raft) doActions(actions ...func()) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, f := range actions {
		f()
	}
}

func (rf *Raft) setRole(role RaftRole, actions ...func()) {
	rf.doActions(append([]func(){func() {
		rf.role = role
	}}, actions...)...)
}

//update term and set role must be atomic
func (rf *Raft) backToFollower(term int, actions ...func()) bool {
	update := false
	rf.mu.Lock()
	if rf.currentTerm < term {
		rf.votedFor = -1
		rf.currentTerm = term
		rf.role = RaftFollower
		update = true
		rf.persist()
	}
	rf.mu.Unlock()
	rf.doActions(actions...)
	return update
}

func (rf *Raft) updateCommitIndex(index int) {
	count := 1
	for _, m := range rf.matchedIndex {
		if m >= index {
			count++
		}
		//println("server", rf.me, "update commitIndex as leader ", index, count, fmt.Sprintf("%+v", rf.matchedIndex))
		//Figure8, section 5.4.2
		if count > len(rf.peers)/2 && rf.logs[rf.logPosition(index)].Term == rf.currentTerm || count == len(rf.peers)-1 {
			rf.commitIndex = index
			//println("server", rf.me, "update commitIndex as leader ", rf.commitIndex)
			return
		}
	}
}

func (rf *Raft) fsm() {
	for {
		switch rf.role {
		case RaftFollower:
			rf.followerState()
			break
		case RaftCandidate:
			rf.candidateState()
			break
		case RaftLeader:
			rf.leaderState()
			break
		case RaftStop:
			RaftDebug("server", rf.me, "stopped!")
			return
		default:
			panic("invalid raft role!")
		}
	}
}

type raftStateOpts struct {
	stateName               string
	timeout                 func() time.Duration
	timeoutAction           func() bool
	requestVoteReqAction    func(*requestVoteReq) bool
	requestVoteRespAction   func(*requestVoteResp) bool
	appendEntriesReqAction  func(*appendEntriesReq) bool
	appendEntriesRespAction func(*appendEntriesResp) bool
}

func (rf *Raft) stateHandler(opts raftStateOpts) {
	now := time.Now()
	timer := time.NewTimer(opts.timeout())
	defer func() {
		timer.Stop()
	}()
	RaftDebug("server", rf.me, "enter", opts.stateName, "now", now)
	for {
		select {
		case <-rf.ctx.Done():
			rf.setRole(RaftStop)
			return
		case req := <-rf.voteReqCh:
			RaftDebug("server", rf.me, "get vote request from", req.args.CandidateId, "in", opts.stateName)
			if opts.requestVoteReqAction(req) {
				return
			}
			break
		case req := <-rf.appendEntriesReqCh:
			RaftDebug("server", rf.me, "get appendEntriesReq request from", req.args.LeaderId, "in", opts.stateName)
			if opts.appendEntriesReqAction(req) {
				return
			}
			break
		case resp := <-rf.voteRespCh:
			RaftDebug("server", rf.me, "get vote resp from", resp.server, "in", opts.stateName)
			if opts.requestVoteRespAction(resp) {
				return
			}
			break
		case resp := <-rf.appendEntriesRespCh:
			RaftDebug("server", rf.me, "get appendEntriesResp from", resp.server, "in", opts.stateName)
			if opts.appendEntriesRespAction(resp) {
				return
			}
			break
		case <-timer.C:
			RaftDebug("server", rf.me, "timeout in", opts.stateName, "duration", time.Since(now), "now", time.Now())
			if opts.timeoutAction() {
				return
			}
			timer.Reset(opts.timeout())
			break
		}
	}
}

func (rf *Raft) followerState() {
	rf.stateHandler(raftStateOpts{
		stateName: "followerState",
		timeout:   rf.getElectionTimeout,
		timeoutAction: func() bool {
			rf.setRole(RaftCandidate)
			return true
		},
		requestVoteReqAction: func(req *requestVoteReq) bool {
			rf.backToFollower(req.args.Term, func() {
				rf.requestVote(req)
			})
			if req.reply.VoteGranted {
				return true
			}
			return false
		},
		requestVoteRespAction: func(resp *requestVoteResp) bool {
			return rf.backToFollower(resp.reply.Term)
		},
		appendEntriesReqAction: func(req *appendEntriesReq) bool {
			rf.backToFollower(req.args.Term, func() {
				rf.appendEntries(req)
			})
			return true
		},
		appendEntriesRespAction: func(resp *appendEntriesResp) bool {
			return rf.backToFollower(resp.reply.Term)
		},
	})
}

func (rf *Raft) candidateState() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leader = -1
	rf.persist()
	rf.mu.Unlock()
	votes := 1
	rf.sendRequestVote()

	rf.stateHandler(raftStateOpts{
		stateName: "candidateState",
		timeout:   rf.getElectionTimeout,
		timeoutAction: func() bool {
			return true
		},
		requestVoteReqAction: func(req *requestVoteReq) bool {
			return rf.backToFollower(req.args.Term, func() {
				rf.requestVote(req)
			})
		},
		requestVoteRespAction: func(resp *requestVoteResp) bool {
			if rf.backToFollower(resp.reply.Term) {
				return true
			}
			if resp.reply.VoteGranted {
				RaftDebug("server", rf.me, "get request vote response and get a vote from", resp.server)
				if rf.currentTerm == resp.reply.Term {
					votes++
					if votes > len(rf.peers)/2 {
						rf.setRole(RaftLeader)
						return true
					}
				}
			}
			return false
		},
		appendEntriesReqAction: func(req *appendEntriesReq) bool {
			rf.backToFollower(req.args.Term, func() {
				rf.appendEntries(req)
			})
			rf.setRole(RaftFollower)
			return true
		},
		appendEntriesRespAction: func(resp *appendEntriesResp) bool {
			return rf.backToFollower(resp.reply.Term)
		},
	})
}

func (rf *Raft) leaderState() {
	rf.mu.Lock()
	//for Figure 8 may cause deadlock when entry leader, and it's discribed in section 8
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logIndex(len(rf.logs))
	}
	rf.matchedIndex = make([]int, len(rf.peers))
	if rf.dummyCmdEn {
		rf.logs = append(rf.logs, RaftLogEntry{DummyRaftCommand, rf.currentTerm})
		rf.persist()
	}
	rf.mu.Unlock()
	rf.sendAppendEntries()

	rf.stateHandler(raftStateOpts{
		stateName: "leaderState",
		timeout: func() time.Duration {
			return RaftHeartBeatPeriod
		},
		timeoutAction: func() bool {
			rf.sendAppendEntries()
			return false
		},
		requestVoteReqAction: func(req *requestVoteReq) bool {
			return rf.backToFollower(req.args.Term, func() {
				//println("server", rf.me, "get vote req from", req.args.CandidateId, "term", req.args.Term)
				rf.requestVote(req)
			})
		},
		requestVoteRespAction: func(resp *requestVoteResp) bool {
			//println("server", rf.me, "get vote resp term", resp.reply.Term)
			return rf.backToFollower(resp.reply.Term)
		},
		appendEntriesReqAction: func(req *appendEntriesReq) bool {
			return rf.backToFollower(req.args.Term, func() {
				//println("server", rf.me, "get append req from", req.args.LeaderId, "term", req.args.Term)
				rf.appendEntries(req)
			})
		},
		appendEntriesRespAction: func(resp *appendEntriesResp) bool {
			if rf.backToFollower(resp.reply.Term) {
				//println("server", rf.me, "get append resp term", resp.reply.Term)
				return true
			}
			if resp.reply.Success {
				rf.setRole(RaftLeader, func() {
					matchedIndex := resp.args.PrevLogIndex + len(resp.args.Entries)
					rf.nextIndex[resp.server] = matchedIndex + 1
					rf.matchedIndex[resp.server] = matchedIndex
					if matchedIndex > rf.commitIndex {
						rf.updateCommitIndex(matchedIndex)
					}
					RaftDebug("server", rf.me, "appendEntries success to", resp.server, "matchedIndex", rf.matchedIndex[resp.server])
				})
				return false
			}
			rf.setRole(RaftLeader, func() {
				if resp.reply.Term > resp.args.Term {
					return
				}
				if resp.reply.ConflictIndex < 1 {
					//fmt.Println("resp.reply.ConflictIndex", resp.reply.ConflictIndex, "resp.reply.Term", resp.reply.Term, "term", rf.currentTerm, "reply from", resp.server, "to", rf.me)
					panic("resp.reply.ConflictIndex < 1 only when leader term < follower term, leader should have return to follower already!")
				}
				if resp.reply.ConflictTerm < 0 {
					rf.nextIndex[resp.server] = resp.reply.ConflictIndex
				} else {
					conflictIndex := resp.reply.ConflictIndex
					for i := rf.nextIndex[resp.server] - 1; i >= 0; i-- {
						if rf.logs[rf.logPosition(i)].Term == resp.reply.ConflictTerm {
							conflictIndex = i + 1
							break
						}
					}
					rf.nextIndex[resp.server] = conflictIndex
				}
				RaftDebug("server", rf.me, "appendEntries fail to", resp.server, "matchedIndex", rf.nextIndex[resp.server], "retry...")
				go rf.sendOneAppendEntries(resp.server)
			})
			return false
		},
	})
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, dummyCmdEn bool) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leader = -1
	rf.dummyCmdEn = dummyCmdEn
	rf.role = RaftFollower
	rf.voteReqCh = make(chan *requestVoteReq, len(rf.peers))
	rf.appendEntriesReqCh = make(chan *appendEntriesReq, len(rf.peers))
	rf.voteRespCh = make(chan *requestVoteResp, len(rf.peers))
	rf.appendEntriesRespCh = make(chan *appendEntriesResp, len(rf.peers))
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []RaftLogEntry{{0, 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.snapshot = RaftSnapShot{
		0,
		0,
		make([]byte, 0),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	rf.ctx, rf.cancel = context.WithCancel(context.Background())

	go rf.fsm()
	go func() {
		for {
			select {
			case <-rf.ctx.Done():
				return
			case <-time.After(RaftHeartBeatPeriod):
				rf.apply(applyCh)
				break
			}
		}
	}()
	return rf
}

func RaftDebug(a ...interface{}) {
	if raftDebug {
		log.Println(a...)
	}
}

func init() {
	flag.BoolVar(&raftDebug, "raft_debug", false, "debug flag of raft")
	flag.Parse()
}
