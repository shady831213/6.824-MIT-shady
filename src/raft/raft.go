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
	"flag"
	"fmt"
	"labrpc"
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

const RaftHeartBeatPeriod = 200 * time.Millisecond

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftLogEntry struct {
	Command interface{}
	Term    int
}

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

	debug bool
	//role state
	role      RaftRole
	heartBeat chan struct{}
	voted     chan struct{}
	newTerm   chan struct{}
	stop      chan struct{}
	// persistent states
	currentTerm int
	votedFor    int
	logs        []RaftLogEntry

	// volatile states
	commitIndex int
	lastApplied int

	// volatile states for leader
	nextIndex    []int
	matchedIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var role RaftRole
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, role = rf.getState()
	return term, role == RaftLeader
}

//must be inside critical region
func (rf *Raft) getState() (int, RaftRole) {
	return rf.currentTerm, rf.role
}

func (rf *Raft) lastLogEntryInfo() (int, int) {
	if len(rf.logs) == 0 {
		return 1, 0
	}
	lastIndex := len(rf.logs)
	lastTerm := rf.logs[lastIndex-1].Term
	return lastIndex, lastTerm
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

//
// example RequestVote RPC handler.
//
//must be inside critical region
func (rf *Raft) updateTerm(term int) bool {
	if rf.currentTerm < term {
		rf.votedFor = -1
		rf.currentTerm = term
		//update role when lock is released
		go func() { rf.newTerm <- struct{}{} }()
		return true
	}
	return false
}

//must be inside critical region
func (rf *Raft) requestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	lastIndex, lastTerm := rf.lastLogEntryInfo()
	reply.Term = rf.currentTerm
	RaftDebug("server term", rf.currentTerm, "request term", args.Term)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	RaftDebug("server votedFor", rf.votedFor, "request id", args.CandidateId)
	if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	RaftDebug("server LastLogTerm", lastTerm, "request LastLogTerm", args.LastLogTerm)
	if args.LastLogTerm < lastTerm {
		reply.VoteGranted = false
		return
	}
	RaftDebug("server LastLogIndex", lastIndex, "request LastLogIndex", args.LastLogIndex)
	if args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex {
		rf.mu.Unlock()
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	RaftDebug("server", rf.me, "vote response", reply.VoteGranted)
	go func() {
		rf.voted <- struct{}{}
		RaftDebug("server", rf.me, "vote to", args.CandidateId)
	}()
}

//the whole rpc must be atomic, then update state
//suppose rpc1 update term, then release lock, and rpc2 get lock update term again,
//rpc1 then get lock and vote, votefor will be not null, then rpc2 get lock again
//rpc2 has newer term but will not get vote, because votefor is not null now

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	RaftDebug("server", rf.me, "get request vote rpc from", args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(args.Term)
	rf.requestVote(args, reply)
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
}

// AppendEntries , currently only heartBeat
func (rf *Raft) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	RaftDebug("server term", rf.currentTerm, "request term", args.Term, "server role", rf.role)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	//fix me
	reply.Success = true
	if args.Entries == nil {
		go func() {
			rf.heartBeat <- struct{}{}
		}()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	RaftDebug("server", rf.me, "get heartbeats rpc from", args.LeaderId)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(args.Term)
	rf.appendEntries(args, reply)
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
func (rf *Raft) sendRequestVote(getVote chan struct{}) {
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				lastIndex, lastTerm := rf.lastLogEntryInfo()
				term, role := rf.getState()
				rf.mu.Unlock()
				if role == RaftCandidate {
					reply := RequestVoteReply{}
					RaftDebug("server", rf.me, "send request vote to", server)
					if ok := rf.peers[server].Call("Raft.RequestVote", &RequestVoteArgs{
						term,
						rf.me,
						lastIndex,
						lastTerm}, &reply); ok {
						//deal response
						RaftDebug("server", rf.me, "get request vote response from", server)
						rf.mu.Lock()
						_, role := rf.getState()
						defer rf.mu.Unlock()
						if rf.updateTerm(reply.Term) {
							RaftDebug("server", rf.me, "get request vote response and to follower from", server)
							return
						}
						if reply.VoteGranted && role == RaftCandidate {
							RaftDebug("server", rf.me, "get request vote response and get a vote from", server)
							go func() {
								getVote <- struct{}{}
							}()
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				term, role := rf.getState()
				rf.mu.Unlock()
				if role == RaftLeader {
					reply := AppendEntriesReply{}
					RaftDebug("server", rf.me, "send heart beats to", server)
					if ok := rf.peers[server].Call("Raft.AppendEntries", &AppendEntriesArgs{
						Term:     term,
						LeaderId: rf.me,
						//fix me
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: 0,
					}, &reply); ok {
						//deal response
						RaftDebug("server", rf.me, "get heart beats response from", server)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.updateTerm(reply.Term) {
							RaftDebug("server", rf.me, "get  heart beats response and to follower from", server)
						}
					}
				}
			}(i)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.stop <- struct{}{}
}

//
// Raft state machine
//

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Int()%500+300) * time.Millisecond
}

func (rf *Raft) setRole(role RaftRole) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
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

func (rf *Raft) followerState() {
	RaftDebug("server", rf.me, "enter followerState")
	timer := time.NewTimer(rf.getElectionTimeout())
	for {
		select {
		case <-rf.stop:
			rf.setRole(RaftStop)
			return
		case <-rf.newTerm:
			return
		case <-timer.C:
			rf.setRole(RaftCandidate)
			return
		case <-rf.heartBeat:
			//if timer has been sendTime already, never get here
			//if timer stop ok, reset
			//if get here, then timer sendTime, stop fail, time.C will be drain immediately.
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(rf.getElectionTimeout())
			break
		case <-rf.voted:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(rf.getElectionTimeout())
			break
		}
	}
}

func (rf *Raft) candidateState() {
	RaftDebug("server", rf.me, "enter candidateState")
	//vote for self
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()
	votes := 1

	//get votes
	getVote := make(chan struct{}, len(rf.peers)-1)
	rf.sendRequestVote(getVote)

	//start timer
	timer := time.NewTimer(rf.getElectionTimeout())
	//change state
	for {
		select {
		case <-rf.stop:
			rf.setRole(RaftStop)
			return
		case <-rf.newTerm:
			rf.setRole(RaftFollower)
			return
		case <-timer.C:
			return
		case <-getVote:
			votes++
			if votes > len(rf.peers)/2 {
				rf.setRole(RaftLeader)
				return
			}
			break
		case <-rf.heartBeat:
			rf.setRole(RaftFollower)
			return
		}
	}
}

func (rf *Raft) leaderState() {
	RaftDebug("server", rf.me, "enter leaderState")
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs) + 1
	}
	rf.matchedIndex = make([]int, len(rf.peers))
	rf.sendHeartBeats()
	for {
		select {
		case <-rf.stop:
			rf.setRole(RaftStop)
			return
		case <-rf.newTerm:
			rf.setRole(RaftFollower)
			RaftDebug("server", rf.me, "exit leaderState")
			return
		case <-time.After(RaftHeartBeatPeriod):
			RaftDebug("server", rf.me, "send heartBeats in leaderState")
			rf.sendHeartBeats()
			break
		}
	}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = RaftFollower
	rf.heartBeat = make(chan struct{})
	rf.voted = make(chan struct{})
	rf.newTerm = make(chan struct{})
	rf.stop = make(chan struct{})
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]RaftLogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.fsm()
	return rf
}

func RaftDebug(a ...interface{}) {
	if raftDebug {
		fmt.Println(a...)
	}
}

func init() {
	flag.BoolVar(&raftDebug, "raft_debug", false, "debug flag of raft")
	flag.Parse()
}
