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
	StageSize    int
	Snapshot     bool
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
	leader                int
	role                  RaftRole
	ctx                   context.Context
	cancel                func()
	startReqCh            chan *startReq
	snapshotReqCh         chan *snapshotReq
	voteReqCh             chan *requestVoteReq
	appendEntriesReqCh    chan *appendEntriesReq
	voteRespCh            chan *requestVoteResp
	appendEntriesRespCh   chan *appendEntriesResp
	installSnapshotReqCh  chan *installSnapshotReq
	installSnapshotRespCh chan *installSnapshotResp
	canApply              chan struct{}
	applyCh               chan ApplyMsg
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
		panic(fmt.Sprint("server ", rf.me, ":", i, " < snapshot index ", rf.snapshot.Index))
	}
	return i - rf.snapshot.Index
}

func (rf *Raft) logIndex(i int) int {
	return i + rf.snapshot.Index
}

//must be inside critical region
func (rf *Raft) lastLogEntryInfo() (int, int) {
	lastIndex := len(rf.logs) - 1
	lastTerm := rf.logs[lastIndex].Term
	return rf.logIndex(lastIndex), lastTerm
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

func (rf *Raft) makeSnapshot(index int, term int, snapshotData []byte) {
	if index <= rf.snapshot.Index {
		return
	}
	if rf.logPosition(index) < len(rf.logs) {
		rf.snapshot.Term = rf.logs[rf.logPosition(index)].Term
		rf.logs = append([]RaftLogEntry{{0, 0}}, rf.logs[rf.logPosition(index+1):]...)
	} else {
		rf.snapshot.Term = term
		rf.logs = []RaftLogEntry{{0, 0}}
	}
	rf.snapshot.Data = snapshotData
	rf.snapshot.Index = index
	rf.persist()
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Int()%10+2) * RaftHeartBeatPeriod
}

func (rf *Raft) apply() {
	RaftDebug("server", rf.me, "begin apply")
	rf.mu.Lock()
	entries := make([]ApplyMsg, 0)
	lastApplied := rf.lastApplied + 1
	RaftDebug("server", rf.me, "apply, lastAppliy", rf.lastApplied, "commitIndex", rf.commitIndex)
	if rf.lastApplied < rf.commitIndex {
		if rf.lastApplied <= rf.snapshot.Index && rf.snapshot.Index != 0 {
			entries = append(entries, ApplyMsg{false, rf.snapshot.Data, rf.snapshot.Index, 0, true})
			lastApplied = rf.snapshot.Index + 1
		}
		if lastApplied <= rf.commitIndex {
			commitEntris := rf.logs[rf.logPosition(lastApplied):rf.logPosition(rf.commitIndex)+1]
			for i, e := range commitEntris {
				if i == len(commitEntris)-1 {
					entries = append(entries, ApplyMsg{e.Command != DummyRaftCommand, e.Command, lastApplied + i, rf.persister.RaftStateSize(), false})
				} else {
					entries = append(entries, ApplyMsg{e.Command != DummyRaftCommand, e.Command, lastApplied + i, 0, false})
				}
			}

		}
		rf.lastApplied = rf.commitIndex
	}
	RaftDebug("server", rf.me, "apply before unlock")
	rf.mu.Unlock()
	for _, entry := range entries {
		rf.applyCh <- entry
	}
	RaftDebug("server", rf.me, "apply done")

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
	rf.startReqCh = make(chan *startReq, 1)
	rf.snapshotReqCh = make(chan *snapshotReq, 1)
	rf.voteReqCh = make(chan *requestVoteReq, len(rf.peers))
	rf.voteRespCh = make(chan *requestVoteResp, len(rf.peers))
	rf.appendEntriesReqCh = make(chan *appendEntriesReq, len(rf.peers))
	rf.appendEntriesRespCh = make(chan *appendEntriesResp, len(rf.peers))
	rf.installSnapshotReqCh = make(chan *installSnapshotReq, len(rf.peers))
	rf.installSnapshotRespCh = make(chan *installSnapshotResp, len(rf.peers))
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
	rf.applyCh = applyCh
	rf.canApply = make(chan struct{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	//if rf.snapshot.Index != 0 {
	//	go rf.applySnapshot()
	//}
	rf.ctx, rf.cancel = context.WithCancel(context.Background())

	go rf.fsm()
	go func() {
		for {
			select {
			case <-rf.ctx.Done():
				return
			case <-rf.canApply:
				RaftDebug("server", rf.me, "got can apply")
				rf.apply()
				RaftDebug("server", rf.me, "can apply done")
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
