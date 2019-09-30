package raft

import (
	"fmt"
	"time"
)

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

func (rf *Raft) sendOneAppendEntries(server int,
	term int,
	lastIndex int,
	lastTerm int,
	commitIndex int,
	entries []RaftLogEntry) {
	RaftDebug("server", rf.me, "before send appendEntries to", server)
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

func (rf *Raft) sendOneInstallSnapshot(server int, term int, snapshot RaftSnapShot) {
	RaftDebug("server", rf.me, "before send installSnapshot to", server)
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: snapshot.Index,
		LastIncludedTerm:  snapshot.Term,
		Data:              snapshot.Data,
	}
	reply := InstallSnapshotReply{}
	RaftDebug("server", rf.me, "send installSnapshot to", server)
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply); ok {
		//deal response
		RaftDebug("server", rf.me, "get installSnapshot response from", server)
		rf.installSnapshotRespCh <- &installSnapshotResp{args: &args, reply: &reply, server: server}
	}
}

func (rf *Raft) sendOneAppendEntriesOrsendOneInstallSnapshot(server int) {
	var entries []RaftLogEntry
	rf.mu.Lock()
	term, role, snapshot := rf.currentTerm, rf.role, rf.snapshot
	lastIndex := rf.nextIndex[server] - 1
	commitIndex := rf.commitIndex
	sendSnapshot := lastIndex < snapshot.Index
	lastTerm := snapshot.Term
	if !sendSnapshot {
		if lastIndex != snapshot.Index {
			lastTerm = rf.logs[rf.logPosition(lastIndex)].Term
		}
		if rf.logIndex(len(rf.logs)-1) > lastIndex {
			entries = append(entries, rf.logs[rf.logPosition(lastIndex+1):]...)
		}
	}
	rf.mu.Unlock()
	if role == RaftLeader {
		if sendSnapshot {
			rf.sendOneInstallSnapshot(server, term, snapshot)
		} else {
			rf.sendOneAppendEntries(server, term, lastIndex, lastTerm, commitIndex, entries)
		}
	}

}

func (rf *Raft) sendAppendEntriesOrInstallSnapshot() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendOneAppendEntriesOrsendOneInstallSnapshot(i)
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
//func (rf *Raft) start(entry RaftLogEntry) int {
//	var index int
//
//	index = rf.logIndex(len(rf.logs))
//	rf.logs = append(rf.logs, entry)
//	rf.persist()
//	RaftDebug("server", rf.me, "start cmd", entry.Command, "logs", rf.logs)
//	//println("server", rf.me, "start cmd", entry.Command, "logs", rf.logs)
//	rf.sendAppendEntriesOrInstallSnapshot()
//	return index
//}

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
	if req.args.PrevLogIndex < rf.snapshot.Index {
		req.reply.Success = false
		req.reply.ConflictIndex = rf.snapshot.Index + 1
		req.reply.ConflictTerm = -1
		return
	}
	//when follower snapshoted and leader have not, because all snapshoted entry must be committed, no way term conflict in snapshot
	if req.args.PrevLogIndex == rf.snapshot.Index && req.args.PrevLogTerm != rf.snapshot.Term {
		fmt.Printf("append args: %+v\n", req.args)
		println()
		fmt.Printf("snapshot: %+v\n", rf.snapshot)
		println()
		panic(fmt.Sprint("server ", rf.me, " get appendEntries rpc from ", req.args.LeaderId, " PrevLogIndex ", req.args.PrevLogIndex, " conflict snapshot term ", req.args.PrevLogTerm, rf.snapshot.Term))
	}

	if req.args.PrevLogIndex > rf.snapshot.Index {
		if entry := rf.logs[rf.logPosition(req.args.PrevLogIndex)]; entry.Term != req.args.PrevLogTerm {
			req.reply.Success = false
			req.reply.ConflictTerm = entry.Term
			for i := rf.logPosition(req.args.PrevLogIndex); i >= 0; i -- {
				if rf.logs[i].Term != req.reply.ConflictTerm {
					req.reply.ConflictIndex = rf.logIndex(i + 1)
					break
				}
			}
			if req.reply.ConflictIndex <= rf.snapshot.Index {
				panic(fmt.Sprint("server", rf.me, "get appendEntries rpc from", req.args.LeaderId, "PrevLogIndex", req.args.PrevLogIndex, "conflict snapshot term", req.args.PrevLogTerm, rf.snapshot.Term))
			}
			return
		}
	}
	//conflict all solved above, just attach
	RaftDebug("server", rf.me, "get appendEntries rpc from", req.args.LeaderId, "can update entries")
	if req.args.PrevLogIndex < rf.logIndex(len(rf.logs)-1) {
		rf.logs = append(rf.logs[:rf.logPosition(req.args.PrevLogIndex+1)], req.args.Entries...)
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
			go rf.applyEntries()
		}
	}
	//println("server", rf.me, "update commitIndex as follower", rf.commitIndex, "log len =", len(rf.logs))
	//fmt.Printf("logs %+v\n", rf.logs)
	//println()
	rf.leader = req.args.LeaderId
	req.reply.Success = true

}

func (rf *Raft) installSnapshot(req *installSnapshotReq) {
	defer func() {
		req.done <- struct{}{}
		close(req.done)
	}()
	RaftDebug("server", rf.me, "term", rf.currentTerm, "request term", req.args.Term, "server role", rf.role)
	req.reply.Term = rf.currentTerm
	if req.args.Term < rf.currentTerm {
		return
	}
	RaftDebug("server", rf.me, "get installSnapshot rpc from", req.args.LeaderId, "LastIncludedIndex", req.args.LastIncludedIndex, "LastIncludedTerm", req.args.LastIncludedTerm)
	if req.args.LastIncludedIndex > rf.snapshot.Index {
		//if rf.logs[rf.logPosition(req.args.LastIncludedIndex)].Term != req.args.LastIncludedTerm {
		//	panic(fmt.Sprint("installSnapshot term conflict,", rf.logs[rf.logPosition(req.args.LastIncludedIndex)].Term, req.args.LastIncludedTerm))
		//}
		println("server", rf.me, "make snapshot though installSnapshot rpc")
		rf.makeSnapshot(req.args.LastIncludedIndex, req.args.LastIncludedTerm, req.args.Data)
		println("server", rf.me, "make snapshot though installSnapshot rpc")
		go rf.applySnapshot()
	}
	if req.args.LastIncludedIndex == rf.snapshot.Index && req.args.LastIncludedTerm != rf.snapshot.Term {
		fmt.Printf("snapshot to be installed: %+v\n", req.args)
		println()
		fmt.Printf("snapshot: %+v\n", rf.snapshot)
		println()
		panic(fmt.Sprint("server ", rf.me, "installSnapshot term conflict from ", req.args.LeaderId, rf.snapshot.Term, req.args.LastIncludedTerm))
	}
	rf.leader = req.args.LeaderId

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
			go rf.applyEntries()
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
	stateName                 string
	timeout                   func() time.Duration
	timeoutAction             func() bool
	startReqAction            func(*startReq) bool
	snapshotReqAction         func(*snapshotReq) bool
	requestVoteReqAction      func(*requestVoteReq) bool
	requestVoteRespAction     func(*requestVoteResp) bool
	appendEntriesReqAction    func(*appendEntriesReq) bool
	appendEntriesRespAction   func(*appendEntriesResp) bool
	installSnapshotReqAction  func(*installSnapshotReq) bool
	installSnapshotRespAction func(resp *installSnapshotResp) bool
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
		case req := <-rf.startReqCh:
			RaftDebug("server", rf.me, "get start request in", opts.stateName)
			if opts.startReqAction(req) {
				return
			}
			break
		case req := <-rf.snapshotReqCh:
			RaftDebug("server", rf.me, "get snapshot request in", opts.stateName)
			if opts.snapshotReqAction(req) {
				return
			}
			break
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
		case req := <-rf.installSnapshotReqCh:
			RaftDebug("server", rf.me, "get installSnapshotReq request from", req.args.LeaderId, "in", opts.stateName)
			if opts.installSnapshotReqAction(req) {
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
		case resp := <-rf.installSnapshotRespCh:
			RaftDebug("server", rf.me, "get installSnapshotResp from", resp.server, "in", opts.stateName)
			if opts.installSnapshotRespAction(resp) {
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
		startReqAction: func(req *startReq) bool {
			rf.mu.Lock()
			req.reply.term = rf.currentTerm
			req.reply.leader = rf.leader
			req.reply.role = RaftFollower
			close(req.done)
			rf.mu.Unlock()
			return false
		},
		snapshotReqAction: func(req *snapshotReq) bool {
			rf.mu.Lock()
			rf.makeSnapshot(req.index, 0, req.data)
			close(req.done)
			rf.mu.Unlock()
			return false
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
		installSnapshotReqAction: func(req *installSnapshotReq) bool {
			rf.backToFollower(req.args.Term, func() {
				rf.installSnapshot(req)
			})
			return true
		},
		installSnapshotRespAction: func(resp *installSnapshotResp) bool {
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
		startReqAction: func(req *startReq) bool {
			rf.mu.Lock()
			req.reply.term = rf.currentTerm
			req.reply.leader = rf.leader
			req.reply.role = RaftCandidate
			close(req.done)
			rf.mu.Unlock()
			return false
		},
		snapshotReqAction: func(req *snapshotReq) bool {
			rf.mu.Lock()
			rf.makeSnapshot(req.index, 0, req.data)
			close(req.done)
			rf.mu.Unlock()
			return false
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
		installSnapshotReqAction: func(req *installSnapshotReq) bool {
			return rf.backToFollower(req.args.Term, func() {
				//println("server", rf.me, "get append req from", req.args.LeaderId, "term", req.args.Term)
				rf.installSnapshot(req)
			})
		},
		installSnapshotRespAction: func(resp *installSnapshotResp) bool {
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
	rf.sendAppendEntriesOrInstallSnapshot()

	rf.stateHandler(raftStateOpts{
		stateName: "leaderState",
		timeout: func() time.Duration {
			return RaftHeartBeatPeriod
		},
		timeoutAction: func() bool {
			rf.sendAppendEntriesOrInstallSnapshot()
			return false
		},
		startReqAction: func(req *startReq) bool {
			rf.mu.Lock()
			req.reply.term = rf.currentTerm
			req.reply.leader = rf.me
			req.reply.role = RaftLeader
			req.reply.index = rf.logIndex(len(rf.logs))
			rf.logs = append(rf.logs, RaftLogEntry{req.command, rf.currentTerm})
			rf.persist()
			close(req.done)
			rf.mu.Unlock()
			rf.sendAppendEntriesOrInstallSnapshot()
			return false
		},
		snapshotReqAction: func(req *snapshotReq) bool {
			rf.mu.Lock()
			rf.makeSnapshot(req.index, 0, req.data)
			close(req.done)
			rf.mu.Unlock()
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
						if rf.logPosition(i) == 0 {
							conflictIndex = rf.snapshot.Index
							break
						}
					}
					rf.nextIndex[resp.server] = conflictIndex
				}
				RaftDebug("server", rf.me, "appendEntries fail to", resp.server, "matchedIndex", rf.nextIndex[resp.server], "retry...")
				go rf.sendOneAppendEntriesOrsendOneInstallSnapshot(resp.server)
			})
			return false
		},
		installSnapshotReqAction: func(req *installSnapshotReq) bool {
			return rf.backToFollower(req.args.Term)
		},
		installSnapshotRespAction: func(resp *installSnapshotResp) bool {
			if rf.backToFollower(resp.reply.Term) {
				//println("server", rf.me, "get append resp term", resp.reply.Term)
				return true
			}
			rf.setRole(RaftLeader, func() {
				rf.nextIndex[resp.server] = rf.snapshot.Index + 1
				go rf.sendOneAppendEntriesOrsendOneInstallSnapshot(resp.server)
			})
			return false
		},
	})
}
