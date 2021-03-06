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

func (rf *Raft) sendOneAppendEntriesOrInstallSnapshot(server int) {
	var entries []RaftLogEntry
	rf.mu.Lock()
	term, role := rf.currentTerm, rf.role
	snapshot := RaftSnapShot{rf.snapshot.Index, rf.snapshot.Term, rf.snapshot.Data}
	lastIndex := rf.nextIndex[server] - 1
	commitIndex := rf.commitIndex
	sendSnapshot := lastIndex < snapshot.Index
	lastTerm := snapshot.Term
	if !sendSnapshot {
		if lastIndex > snapshot.Index {
			lastTerm = rf.logs[lastIndex-snapshot.Index].Term
		}
		if rf.logIndex(len(rf.logs)-1) > lastIndex {
			entries = append(entries, rf.logs[lastIndex-snapshot.Index+1:]...)
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

func (rf *Raft) sendAppendEntriesOrInstallSnapshot(delay time.Duration) {
	time.Sleep(delay)
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendOneAppendEntriesOrInstallSnapshot(i)
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
	if req.args.PrevLogIndex == rf.snapshot.Index && req.args.PrevLogTerm != rf.snapshot.Term {
		req.reply.Success = false
		req.reply.ConflictIndex = rf.snapshot.Index + 1
		req.reply.ConflictTerm = -1
		return
	}

	if req.args.PrevLogIndex > rf.snapshot.Index {
		if entry := rf.logs[rf.logPosition(req.args.PrevLogIndex)]; entry.Term != req.args.PrevLogTerm {
			req.reply.Success = false
			req.reply.ConflictTerm = entry.Term
			for i := rf.logPosition(req.args.PrevLogIndex); i > 0; i -- {
				if rf.logs[i].Term != req.reply.ConflictTerm {
					req.reply.ConflictIndex = rf.logIndex(i + 1)
					return
				}
			}
			req.reply.ConflictIndex = rf.snapshot.Index + 1
			return
		}
	}

	if req.args.PrevLogIndex == rf.snapshot.Index && req.args.PrevLogTerm != rf.snapshot.Term{
		req.reply.Success = false
		req.reply.ConflictTerm = rf.snapshot.Term
		req.reply.ConflictIndex = rf.snapshot.Index
		return
	}

	//conflict all solved above, just attach
	RaftDebug("server", rf.me, "get appendEntries rpc from", req.args.LeaderId, "can update entries")
	if req.args.PrevLogIndex < rf.logIndex(len(rf.logs)-1) {
		for i, e := range req.args.Entries{
			if rf.logPosition(req.args.PrevLogIndex+i+1) > len(rf.logs) - 1 || e.Term != rf.logs[rf.logPosition(req.args.PrevLogIndex+i+1)].Term {
				rf.logs = append(rf.logs[:rf.logPosition(req.args.PrevLogIndex+i+1)], req.args.Entries[i:]...)
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
		RaftDebug("server", rf.me, "update commitIndex through appendEntries success from", req.args.LeaderId, rf.commitIndex, req.args.LeaderCommit)
		go func() {
			rf.canApply <- struct{}{}
		}()
	}
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
	defer func() {
		//update commitIndex
		if req.args.LastIncludedIndex > rf.commitIndex {
			rf.commitIndex = req.args.LastIncludedIndex
			RaftDebug("server", rf.me, "update commitIndex through installSnapshot success from", req.args.LeaderId, rf.commitIndex, req.args.LastIncludedIndex)
			go func() {
				rf.canApply <- struct{}{}
			}()
		}
		if rf.logPosition(rf.commitIndex) > len(rf.logs)-1 {
			panic(fmt.Sprintf("%s%d update commit to %d log %+v snapshotindex %d", rf.Tag, rf.me, rf.commitIndex, rf.logs, rf.snapshot.Index))
		}
		rf.leader = req.args.LeaderId
	}()
	for i, e := range rf.logs {
		if e.Term == req.args.LastIncludedTerm && rf.logIndex(i) == req.args.LastIncludedIndex {
			return
		}
	}
	if req.args.LastIncludedIndex < rf.logIndex(len(rf.logs)-1) {
		if  req.args.LastIncludedIndex < rf.snapshot.Index {
			return
		}
		rf.logs = append([]RaftLogEntry{{0, 0}}, rf.logs[rf.logPosition(req.args.LastIncludedIndex+1):]...)
	} else {
		rf.logs = []RaftLogEntry{{0, 0}}
	}
	rf.snapshot.Term = req.args.LastIncludedTerm
	rf.snapshot.Index = req.args.LastIncludedIndex
	rf.snapshot.Data = req.args.Data
	rf.persist()
}

func (rf *Raft) doActions(actions ...func()) {
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
	if rf.currentTerm < term {
		rf.votedFor = -1
		rf.currentTerm = term
		rf.role = RaftFollower
		update = true
		rf.persist()
	}
	rf.doActions(actions...)
	return update
}

func (rf *Raft) updateCommitIndex(index int) {
	count := 1
	for _, m := range rf.matchedIndex {
		if m >= index {
			count++
		}
		RaftDebug("server", rf.me, "update commitIndex as leader ", index, count, fmt.Sprintf("%+v", rf.matchedIndex))
		//Figure8, section 5.4.2
		if count > len(rf.peers)/2 && rf.logs[rf.logPosition(index)].Term == rf.currentTerm || count == len(rf.peers)-1 {
			rf.commitIndex = index
			go func() {
				rf.canApply <- struct{}{}
			}()
			RaftDebug("server", rf.me, "update commitIndex as leader ", rf.commitIndex)
			if rf.logPosition(rf.commitIndex) > len(rf.logs)-1 {
				panic(fmt.Sprintf("%s%d update commit to %d log %+v snapshotindex %d", rf.Tag, rf.me, rf.commitIndex, rf.logs, rf.snapshot.Index))
			}
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
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.setRole(RaftStop)
			return
		case req := <-rf.startReqCh:
			rf.mu.Lock()
			RaftDebug("server", rf.me, "get start request in", opts.stateName)
			if opts.startReqAction(req) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			break
		case req := <-rf.snapshotReqCh:
			rf.mu.Lock()
			RaftDebug("server", rf.me, "get snapshot request in", opts.stateName)
			if opts.snapshotReqAction(req) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			break
		case req := <-rf.voteReqCh:
			rf.mu.Lock()
			RaftDebug("server", rf.me, "get vote request from", req.args.CandidateId, "in", opts.stateName)
			if opts.requestVoteReqAction(req) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			break
		case req := <-rf.appendEntriesReqCh:
			rf.mu.Lock()
			RaftDebug("server", rf.me, "get appendEntriesReq request from", req.args.LeaderId, "in", opts.stateName)
			if opts.appendEntriesReqAction(req) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			break
		case req := <-rf.installSnapshotReqCh:
			rf.mu.Lock()
			RaftDebug("server", rf.me, "get installSnapshotReq request from", req.args.LeaderId, "in", opts.stateName)
			if opts.installSnapshotReqAction(req) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			break
		case resp := <-rf.voteRespCh:
			rf.mu.Lock()
			RaftDebug("server", rf.me, "get vote resp from", resp.server, "in", opts.stateName)
			if opts.requestVoteRespAction(resp) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			break
		case resp := <-rf.appendEntriesRespCh:
			rf.mu.Lock()
			RaftDebug("server", rf.me, "get appendEntriesResp from", resp.server, "in", opts.stateName)
			if opts.appendEntriesRespAction(resp) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			break
		case resp := <-rf.installSnapshotRespCh:
			rf.mu.Lock()
			RaftDebug("server", rf.me, "get installSnapshotResp from", resp.server, "in", opts.stateName)
			if opts.installSnapshotRespAction(resp) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			break
		case <-timer.C:
			rf.mu.Lock()
			RaftDebug("server", rf.me, "timeout in", opts.stateName, "duration", time.Since(now), "now", time.Now())
			if opts.timeoutAction() {
				rf.mu.Unlock()
				return
			}
			timer.Reset(opts.timeout())
			rf.mu.Unlock()
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
			rf.doActions(func() {
				req.reply.term = rf.currentTerm
				req.reply.leader = rf.leader
				req.reply.role = RaftFollower
				close(req.done)
			})
			return false
		},
		snapshotReqAction: func(req *snapshotReq) bool {
			rf.doActions(func() {
				rf.makeSnapshot(req.index, req.data)
				close(req.done)
			})
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
			rf.doActions(func() {
				req.reply.term = rf.currentTerm
				req.reply.leader = rf.leader
				req.reply.role = RaftCandidate
				close(req.done)
			})
			return false
		},
		snapshotReqAction: func(req *snapshotReq) bool {
			rf.doActions(func() {
				rf.makeSnapshot(req.index, req.data)
				close(req.done)
			})
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
	rf.sendAppendEntriesOrInstallSnapshot(0)

	rf.stateHandler(raftStateOpts{
		stateName: "leaderState",
		timeout: func() time.Duration {
			return RaftHeartBeatPeriod
		},
		timeoutAction: func() bool {
			rf.doActions(func() {
				rf.sendAppendEntriesOrInstallSnapshot(0)
			})
			return false
		},
		startReqAction: func(req *startReq) bool {
			rf.doActions(func() {
				req.reply.term = rf.currentTerm
				req.reply.leader = rf.me
				req.reply.role = RaftLeader
				req.reply.index = rf.logIndex(len(rf.logs))
				close(req.done)
				rf.logs = append(rf.logs, RaftLogEntry{req.command, rf.currentTerm})
				rf.persist()
			})
			go rf.sendAppendEntriesOrInstallSnapshot(1 * time.Microsecond)
			return false
		},
		snapshotReqAction: func(req *snapshotReq) bool {
			rf.doActions(func() {
				rf.makeSnapshot(req.index, req.data)
				close(req.done)
			})
			return false
		},
		requestVoteReqAction: func(req *requestVoteReq) bool {
			return rf.backToFollower(req.args.Term, func() {
				rf.requestVote(req)
			})
		},
		requestVoteRespAction: func(resp *requestVoteResp) bool {
			return rf.backToFollower(resp.reply.Term)
		},
		appendEntriesReqAction: func(req *appendEntriesReq) bool {
			if rf.backToFollower(req.reply.Term) {
				rf.doActions(func() {
					rf.appendEntries(req)
				})
				return true
			}
			return false
		},
		appendEntriesRespAction: func(resp *appendEntriesResp) bool {
			if rf.backToFollower(resp.reply.Term) {
				return true
			}
			if resp.reply.Success {
				rf.setRole(RaftLeader, func() {
					matchedIndex := resp.args.PrevLogIndex + len(resp.args.Entries)
					if 	matchedIndex+1 > rf.nextIndex[resp.server] {
						rf.nextIndex[resp.server] = matchedIndex + 1
					}
					if 	matchedIndex > rf.matchedIndex[resp.server] {
						rf.matchedIndex[resp.server] = matchedIndex
					}
					if matchedIndex > rf.commitIndex {
						rf.updateCommitIndex(matchedIndex)
					}
					RaftDebug("server", rf.me, "appendEntries success to", resp.server, "matchedIndex", rf.matchedIndex[resp.server], rf.commitIndex)
				})
				return false
			}
			rf.setRole(RaftLeader, func() {
				if resp.reply.Term > resp.args.Term {
					return
				}
				if resp.reply.ConflictIndex < 1 {
					panic("resp.reply.ConflictIndex < 1 only when leader term < follower term, leader should have return to follower already!")
				}
				if resp.reply.ConflictTerm < 0 || resp.reply.ConflictIndex < rf.snapshot.Index {
					rf.nextIndex[resp.server] = resp.reply.ConflictIndex
				} else {
					conflictIndex := resp.reply.ConflictIndex
					for i := rf.nextIndex[resp.server] - 1; i >= 0; i-- {
						if i <= rf.snapshot.Index {
							conflictIndex = rf.snapshot.Index
							break
						}
						if rf.logs[rf.logPosition(i)].Term == resp.reply.ConflictTerm {
							conflictIndex = i + 1
							break
						}

					}
					rf.nextIndex[resp.server] = conflictIndex
				}
				RaftDebug("server", rf.me, "appendEntries fail to", resp.server, "matchedIndex", rf.nextIndex[resp.server], "retry...")
				go rf.sendOneAppendEntriesOrInstallSnapshot(resp.server)
			})
			return false
		},
		installSnapshotReqAction: func(req *installSnapshotReq) bool {
			return rf.backToFollower(req.args.Term)
		},
		installSnapshotRespAction: func(resp *installSnapshotResp) bool {
			if rf.backToFollower(resp.reply.Term) {
				return true
			}
			rf.setRole(RaftLeader, func() {
				if resp.args.LastIncludedIndex+1 > rf.nextIndex[resp.server] {
					rf.nextIndex[resp.server] = resp.args.LastIncludedIndex + 1
				}
				go rf.sendOneAppendEntriesOrInstallSnapshot(resp.server)
			})
			return false
		},
	})
}
