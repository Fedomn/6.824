package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state StateType

	currentTerm int
	votedFor    int        // voted for candidate's id
	log         []LogEntry // first index is 1

	commitIndex int
	lastApplied int

	nextIndex  []int // each peer next send log entry index
	matchIndex []int // each peer already replicated the highest log entry index

	electionElapsed           int
	heartbeatElapsed          int
	heartbeatTimeout          int
	electionTimeout           int
	randomizedElectionTimeout int

	lead     int
	tick     func()
	step     func(r *Raft, e Event) error
	voteFrom map[int]struct{}

	// for internal event chan
	eventCh chan Event

	killCh chan struct{}

	// 为了解决preCandidate/candidate/leader sendRPC 到 unreliable网络，follower reply乱序
	// 造成EventPreVoteResp/EventVoteResp/EventAppResp/EventSnapResp 乱序，影响raft判断，尤其是leader计算nextIndex时
	// 设计成 只处理 最新的 发出去的 RPC response，其它情况 直接丢弃response
	sendRpcLatestSeq map[int]*RpcSeqStatus

	// 为了解决preCandidate/candidate/leader sendRPC 到 unreliable网络，follower receive乱序
	// 造成EventVote/EventPreVote/EventAppEventSnap 乱序，影响raft判断，尤其是follower会无条件地用leader发来的entries来overwrite自己
	// 设计成 只处理 最新的 发来的 RPC request，并记录seq来实现 老的request seq直接丢弃
	recvRpcLatestSeq map[int]uint32

	// for tester
	applyCh chan ApplyMsg
	// for internal async apply
	applyCond *sync.Cond

	replicatorTrigger chan struct{}

	asyncSnapshotCh chan ApplyMsg
}

type RpcSeqStatus struct {
	SendSeq uint32
	RecvSeq uint32
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) GetLeader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lead
}

func (rf *Raft) persist() {
	data := rf.encodeRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.sendRpcLatestSeq) != nil ||
		e.Encode(rf.recvRpcLatestSeq) != nil {
		panic("persist encounter error")
	}
	data := w.Bytes()
	return data
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var x int
	var y int
	var z []LogEntry
	var a map[int]*RpcSeqStatus
	var b map[int]uint32
	if d.Decode(&x) != nil ||
		d.Decode(&y) != nil ||
		d.Decode(&z) != nil ||
		d.Decode(&a) != nil ||
		d.Decode(&b) != nil {
		panic("readPersist encounter error")
	} else {
		rf.currentTerm = x
		rf.votedFor = y
		rf.log = z
		rf.sendRpcLatestSeq = a
		rf.recvRpcLatestSeq = b
	}
}

// raft上层应用会调用它，来判断是否需要install snapshot，并设置raft state
// 这里也可以一致返回true，这样上层应用会重复install snapshot
// 正常能够调用这个方法由于，向applyCh里放入SnapshotValid类型的ApplyMsg，也就是follower被leader主动InstallSnapshot RPC后
func (rf *Raft) CondInstallSnapshot(snapshotTerm int, snapshotIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(rf.me, "CondInstallSnapshot begin, snapshotIndex:%v commitIndex:%v lastApplied:%v",
		snapshotIndex, rf.commitIndex, rf.lastApplied)
	if snapshotIndex <= rf.commitIndex {
		// 一旦进入commitIndex，最终都会被apply，因此也不需要install snapshot了
		DPrintf(rf.me, "CondInstallSnapshot outdated snapshotIndex:%v <= commitIndex:%v, no need to install snapshot", snapshotIndex, rf.commitIndex)
		return false
	}

	// rf.commitIndex < snapshotIndex
	if snapshotIndex > rf.getLastLogIndex() {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = rf.getEntriesToEnd(snapshotIndex)
	}
	rf.log[0].Command = nil
	rf.log[0].Term = snapshotTerm
	rf.log[0].Index = snapshotIndex
	rf.commitIndex = snapshotIndex
	rf.lastApplied = snapshotIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
	DPrintf(rf.me, "CondInstallSnapshot installed snapshot success, log:%v, commitIndex:%v", rf.log, rf.commitIndex)
	return true
}

// 这里只传一个snapshot的原因：简化kv模型，由于test里每次cfg.one一个值，相当于对一个相同的key不停的set value
// 因此，我们只需要设置最后一次的value=snapshot就可以
// 能够调用这个方法是由于，向applyCh里放入CommandValid类型的ApplyMsg，达到一定size则执行，不论raft是什么state
func (rf *Raft) Snapshot(snapshotIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(rf.me, "Snapshot begin, snapshotIndex:%v", snapshotIndex)

	// 如果snapshot index在commitIndex之后，说明还不能打snapshot，必须是committed的log才能认为可以snapshot
	if snapshotIndex > rf.commitIndex {
		DPrintf(rf.me, "Snapshot snapshotIndex:%v > commitIndex:%v, so reject this snapshot operation",
			snapshotIndex, rf.commitIndex)
		return
	}

	// shrink logs
	rf.log = rf.getEntriesToEnd(snapshotIndex)
	rf.log[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
	DPrintf(rf.me, "Snapshot snapshotIndex:%v snapshot:%v, after log:%v", snapshotIndex, binary.BigEndian.Uint32(snapshot), rf.log)
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = -1
	term = -1
	isLeader = rf.state == StateLeader
	if !isLeader {
		return
	}

	newLogEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.getLastLogIndex() + 1,
	}
	rf.log = append(rf.log, newLogEntry)
	index = newLogEntry.Index
	term = newLogEntry.Term
	rf.persist()
	rf.replicatorTrigger <- struct{}{}
	return
}

func (rf *Raft) replicator() {
	for {
		select {
		case <-rf.replicatorTrigger:
			rf.mu.Lock()
			isLeader := rf.state == StateLeader
			e := Event{Type: EventReplicate, From: rf.me, Term: rf.currentTerm}
			rf.mu.Unlock()
			if isLeader {
				rf.mu.Lock()
				DPrintf(rf.me, "Leader proposeT immediately")
				rf.heartbeatElapsed = 0
				rf.mu.Unlock()
				rf.send(e)
			}
		case <-rf.killCh:
			return
		}
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.killCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) becomeFollower(term int, lead int) {
	rf.reset(term)
	rf.state = StateFollower
	rf.tick = rf.tickElection
	rf.step = stepFollower
	rf.lead = lead
	DPrintf(rf.me, "Raft %v became follower at term %v", rf.me, rf.currentTerm)
	rf.persist()
}
func (rf *Raft) becomePreCandidate() {
	if rf.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// 注意这里：preCandidate term是不会递增的
	rf.reset(rf.currentTerm)
	rf.state = StatePreCandidate
	rf.votedFor = rf.me
	rf.voteFrom[rf.me] = struct{}{}
	rf.tick = rf.tickElection
	rf.step = stepPreCandidate
	DPrintf(rf.me, "Raft %v became pre-candidate at term %v", rf.me, rf.currentTerm)
	rf.persist()
}
func (rf *Raft) becomeCandidate() {
	if rf.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	rf.reset(rf.currentTerm + 1)
	rf.state = StateCandidate
	rf.votedFor = rf.me
	rf.voteFrom[rf.me] = struct{}{}
	rf.tick = rf.tickElection
	rf.step = stepCandidate
	DPrintf(rf.me, "Raft %v became candidate at term %v", rf.me, rf.currentTerm)
	rf.persist()
}
func (rf *Raft) becomeLeader() {
	if rf.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	rf.reset(rf.currentTerm)
	rf.state = StateLeader
	rf.tick = rf.tickHeartbeat
	rf.step = stepLeader
	rf.lead = rf.me

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}

	DPrintf(rf.me, "Raft %v became leader at term %v, "+
		"commitIndex:%v, lastApplied:%v, nextIndex:%v, matchIndex:%v, Last3Logs:%v",
		rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, debugLast3Logs(rf.log))
	rf.persist()
}

func (rf *Raft) Step(e Event) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		if e.DoneC != nil {
			close(e.DoneC)
		}
	}()

	switch e.Type {
	case EventVote, EventPreVote:
		args := e.Args.(*RequestVoteArgs)
		reply := e.Reply.(*RequestVoteReply)
		if args.Seq <= rf.recvRpcLatestSeq[e.From] {
			DRpcPrintf(rf.me, args.Seq, "%s %v<-%v AbortOldRPC argsSeq:%v <= recvRpcLatestSeq:%v, ignore it", e.Type, rf.me, args.CandidateId, args.Seq, rf.recvRpcLatestSeq[e.From])
			return nil
		}
		rf.setRecvRpcLatestSeq(e.From, args.Seq)

		switch {
		case args.Term < rf.currentTerm:
			DRpcPrintf(rf.me, args.Seq, "%s %v<-%v currentTerm %v > term %v, ignore lower term", e.Type, rf.me, args.CandidateId, rf.currentTerm, args.Term)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		case args.Term == rf.currentTerm:
			if rf.votedFor != None && rf.votedFor != args.CandidateId {
				DRpcPrintf(rf.me, args.Seq, "%s %v<-%v currentTerm %v == term %v, already votedFor %v", e.Type, rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.votedFor)
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			} else {
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				DRpcPrintf(rf.me, args.Seq, "%s %v<-%v currentTerm %v == term %v, haven't vote, will votedFor %v", e.Type, rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.votedFor)
			}
		case args.Term > rf.currentTerm:
			passCheck := true
			for loop := true; loop; loop = false {
				lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
				if args.LastLogTerm < lastLogTerm {
					passCheck = false
					DRpcPrintf(rf.me, args.Seq, "%s %v<-%v fail election restriction check about term. %v < %v", e.Type, rf.me, args.CandidateId, args.LastLogTerm, lastLogTerm)
					break
				}

				if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
					passCheck = false
					DRpcPrintf(rf.me, args.Seq, "%s %v<-%v fail election restriction check about index when same term. %v < %v", e.Type, rf.me, args.CandidateId, args.LastLogIndex, lastLogIndex)
					break
				}
			}

			// 注意preVote不会影响raft状态
			if e.Type == EventPreVote {
				reply.VoteGranted = passCheck
				reply.Term = rf.currentTerm
				DRpcPrintf(rf.me, args.Seq, "%s %v<-%v reply:%+v", e.Type, rf.me, args.CandidateId, reply)
				break
			}

			originalCurrentTerm := rf.currentTerm
			rf.currentTerm = args.Term
			if passCheck {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				reply.Term = rf.currentTerm
			} else {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
			}

			if rf.state != StateFollower {
				DRpcPrintf(rf.me, args.Seq, "%s %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately", e.Type, rf.me, args.CandidateId, rf.state, originalCurrentTerm, reply.Term)
				rf.becomeFollower(args.Term, None)
			}
		}
		if reply.VoteGranted {
			rf.electionElapsed = 0
		}
		return nil
	case EventApp:
		args := e.Args.(*AppendEntriesArgs)
		reply := e.Reply.(*AppendEntriesReply)
		if args.Seq <= rf.recvRpcLatestSeq[e.From] {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v AbortOldRPC argsSeq:%v <= recvRpcLatestSeq:%v, ignore it", rf.me, args.LeaderId, args.Seq, rf.recvRpcLatestSeq[e.From])
			return nil
		}
		rf.setRecvRpcLatestSeq(e.From, args.Seq)

		switch {
		case args.Term < rf.currentTerm:
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v currentTerm %v > term %v, ignore lower term", rf.me, args.LeaderId, rf.currentTerm, args.Term)
			reply.Term = rf.currentTerm
			reply.Success = false
		case args.Term >= rf.currentTerm:
			if rf.state != StateFollower {
				DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately", rf.me, args.LeaderId, rf.state, rf.currentTerm, args.Term)
				rf.becomeFollower(args.Term, args.LeaderId)
			}
			// consistency check：prevLogIndex所在的log entry，它的term不等于prevLogTerm。
			passCheck := true
			for loop := true; loop; loop = false {
				// 原因：虽然follower认为已经committed的log，但整个集群并不认为，所以每次需要leader的overwrite
				// 一致性检测 和 leaderCommit 设置没有关系，一致性检测用来 check是否和leader是up-to-date的
				lastLogIndex := rf.getLastLogIndex()
				if args.PrevLogIndex > lastLogIndex {
					// 防止slice越界
					passCheck = false
					// 注意，conflict index应该为最后的index + 1，因为在下一次RPC需要计算PrevLogIndex=nextIndex-1
					reply.ConflictIndex = lastLogIndex + 1
					DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v fail consistency for index. %v < %v, conflictIndex:%v", rf.me, args.LeaderId, lastLogIndex, args.PrevLogIndex, reply.ConflictIndex)
					break
				}

				// Optimization: when rejecting an AppendEntries request, the follower can include the term of
				// the conflicting entry and the first index it stores for that term.
				// With this information, the leader can decrement nextIndex to bypass all the conflicting entries in that term
				// one AppendEntries RPC will be required for each term with conflicting entries, rather than one RPC per entry
				matchedIndexLogTerm := rf.getLogEntry(args.PrevLogIndex).Term
				if matchedIndexLogTerm != args.PrevLogTerm {
					passCheck = false
					reply.ConflictIndex = rf.getFirstIndexOfTerm(matchedIndexLogTerm)
					DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v fail consistency for term. %v != %v, conflictIndex:%v", rf.me, args.LeaderId, matchedIndexLogTerm, args.PrevLogTerm, reply.ConflictIndex)
					break
				}
			}
			if passCheck {
				rf.currentTerm = args.Term
				reply.Term = rf.currentTerm
				reply.Success = true

				newLog := make([]LogEntry, 0)
				for _, entry := range args.Entries {
					logEntry := LogEntry{
						Command: entry.Command,
						Term:    entry.Term,
						Index:   entry.Index,
					}
					newLog = append(newLog, logEntry)
				}
				rf.log = append(rf.log[:rf.getLogEntryIndex(args.PrevLogIndex)+1], newLog...)

				// 如果leader commit index > follower 本地存储的 commit index，
				// 则更新 follower本地的 commitIndex = min(leaderCommit , 将要保存的logs中最后一个log entry index)
				if args.LeaderCommit > rf.commitIndex {
					// 存在一种情况，follower落后leader很多，这次appendEntries还未补全所有log，
					// 所以 这次follower的committedIndex为最后一个logIndex
					rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())

					// 真正的异步apply在 asyncApplier
					rf.applyCond.Signal()
				}
			} else {
				reply.Success = false
				reply.Term = rf.currentTerm
			}
		}
		if reply.Success {
			rf.electionElapsed = 0
			rf.lead = args.LeaderId
		}
		return nil
	case EventSnap:
		args := e.Args.(*InstallSnapshotArgs)
		reply := e.Reply.(*InstallSnapshotReply)
		DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v receiveEventSnap args:%v", rf.me, args.LeaderId, args)
		if args.Seq <= rf.recvRpcLatestSeq[e.From] {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v AbortOldRPC argsSeq:%v <= recvRpcLatestSeq:%v, ignore it", rf.me, args.LeaderId, args.Seq, rf.recvRpcLatestSeq[e.From])
			return nil
		}
		rf.setRecvRpcLatestSeq(e.From, args.Seq)

		switch {
		case args.Term < rf.currentTerm:
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v currentTerm %v > term %v, ignore lower term", rf.me, args.LeaderId, rf.currentTerm, args.Term)
			reply.Term = rf.currentTerm
			reply.Success = false
		case args.Term >= rf.currentTerm:
			if rf.state != StateFollower {
				DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately", rf.me, args.LeaderId, rf.state, rf.currentTerm, args.Term)
				rf.becomeFollower(args.Term, args.LeaderId)
			}
			// 这里不需要consistency check，因为一致性检测的结果是为了计算conflictIndex，让leader快速设置nextIndex，继续下一层appendEntries RPC
			// 但对于InstallSnapshot来说，不需要conflictIndex，因为follower要无条件接受leader的snapshot
			reply.Term = args.Term
			reply.Success = true

			// 这里apply snapshot，等待上层CondInstallSnapshot成功后，apply到应用层
			rf.asyncSnapshotCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
		}
		if reply.Success {
			rf.electionElapsed = 0
			rf.lead = args.LeaderId
		}
		return nil
	default:
		return rf.step(rf, e)
	}
}
func stepFollower(rf *Raft, e Event) error {
	switch e.Type {
	case EventPreHup:
		rf.becomePreCandidate()
		rf.send(e)
	default:
		DPrintf(rf.me, fmt.Sprintf("ignore event:%v for %v", e, rf.state))
	}
	return nil
}
func stepPreCandidate(rf *Raft, e Event) error {
	switch e.Type {
	case EventPreHup:
		rf.startPreRequestVote()
	case EventPreVoteResp:
		args := e.Args.(*RequestVoteArgs)
		reply := e.Reply.(*RequestVoteReply)
		if args.Seq != rf.sendRpcLatestSeq[e.From].SendSeq {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v AbortOldRPC argsSeq:%v != sendRpcLatestSeq:%v, ignore it", e.From, e.To, args.Seq, rf.sendRpcLatestSeq[e.From].SendSeq)
			return nil
		}
		rf.sendRpcLatestSeq[e.From].RecvSeq = args.Seq

		if reply == nil {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v gotRPC nil, reply:%v", e.From, e.To, reply)
			return nil
		}
		if reply.Term > rf.currentTerm {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.VoteGranted {
			rf.voteFrom[e.From] = struct{}{}
		}
		// got majority votes
		if len(rf.voteFrom) >= len(rf.peers)/2+1 {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v voteFrom %+v", e.From, e.To, rf.voteFrom)
			rf.becomeCandidate()
			rf.send(Event{Type: EventHup, From: rf.me, To: rf.me, Term: rf.currentTerm})
			return nil
		}
	default:
		DPrintf(rf.me, fmt.Sprintf("ignore event:%v for raft:%v", e, rf.state))
	}
	return nil
}
func stepCandidate(rf *Raft, e Event) error {
	switch e.Type {
	case EventHup:
		rf.startRequestVote()
	case EventVoteResp:
		args := e.Args.(*RequestVoteArgs)
		reply := e.Reply.(*RequestVoteReply)
		if args.Seq != rf.sendRpcLatestSeq[e.From].SendSeq {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v AbortOldRPC argsSeq:%v != sendRpcLatestSeq:%v, ignore it", e.From, e.To, args.Seq, rf.sendRpcLatestSeq[e.From].SendSeq)
			return nil
		}
		rf.sendRpcLatestSeq[e.From].RecvSeq = args.Seq

		if reply == nil {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v gotRPC nil, reply:%v", e.From, e.To, reply)
			return nil
		}
		if reply.Term > rf.currentTerm {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.VoteGranted {
			rf.voteFrom[e.From] = struct{}{}
		}
		// got majority votes
		if len(rf.voteFrom) >= len(rf.peers)/2+1 {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v voteFrom %+v", e.From, e.To, rf.voteFrom)
			rf.becomeLeader()
			//rf.log = append(rf.log, LogEntry{
			//	Command: "NoOp",
			//	Term:    rf.currentTerm,
			//	Index:   rf.getLastLogIndex() + 1,
			//})
			rf.send(Event{Type: EventBeat, From: rf.me, Term: rf.currentTerm})
			return nil
		}
		// split vote的情况处理逻辑，发生在electionTimeout里，避免这里还需要计数requestedVoteCount
		// 同时我们基于event-driven后，channel是顺序取event，就出现了requestedVoteCount已经=3了，但状态机还未收到event3
	default:
		DPrintf(rf.me, fmt.Sprintf("ignore event:%v for raft:%v", e, rf.state))
	}
	return nil
}
func stepLeader(rf *Raft, e Event) error {
	switch e.Type {
	case EventBeat:
		rf.startAppendEntries(true)
	case EventReplicate:
		rf.startAppendEntries(false)
	case EventAppResp:
		args := e.Args.(*AppendEntriesArgs)
		reply := e.Reply.(*AppendEntriesReply)
		if args.Seq != rf.sendRpcLatestSeq[e.From].SendSeq {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v AbortOldRPC argsSeq:%v != sendRpcLatestSeq:%v, ignore it", e.From, e.To, args.Seq, rf.sendRpcLatestSeq[e.From].SendSeq)
			return nil
		}
		rf.sendRpcLatestSeq[e.From].RecvSeq = args.Seq

		if reply == nil {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v gotRPC nil, reply:%v", e.From, e.To, reply)
			return nil
		}
		if reply.Term > rf.currentTerm {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.Success {
			if len(args.Entries) == 0 {
				DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v gotRPC success, entries len: %v, so heartbeat will do nothing", e.From, e.To, len(args.Entries))
			} else {
				rf.setNextIndexAndMatchIndex(e.From, len(args.Entries))
				DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v gotRPC success, entries len: %v, so had increased nextIndex %v, matchIndex %v", e.From, e.To, len(args.Entries), rf.nextIndex, rf.matchIndex)
			}
		} else {
			rf.nextIndex[e.From] = reply.ConflictIndex
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v gotRPC false, so will decrease nextIndex and append again, %v", e.From, e.To, rf.nextIndex)
			return nil
		}

		// maybeCommit
		calcCommitIndex := rf.calcCommitIndex()
		if rf.commitIndex != calcCommitIndex {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v leader maybe commit, before:%v after calc commitIndex:%v, matchIndex: %v", e.From, e.To, rf.commitIndex, calcCommitIndex, rf.matchIndex)
			rf.commitIndex = calcCommitIndex

			// 真正的异步apply在 asyncApplier
			rf.applyCond.Signal()
		}
	case EventSnapResp:
		args := e.Args.(*InstallSnapshotArgs)
		reply := e.Reply.(*InstallSnapshotReply)
		if args.Seq != rf.sendRpcLatestSeq[e.From].SendSeq {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v AbortOldRPC argsSeq:%v != sendRpcLatestSeq:%v, ignore it", e.From, e.To, args.Seq, rf.sendRpcLatestSeq[e.From].SendSeq)
			return nil
		}
		rf.sendRpcLatestSeq[e.From].RecvSeq = args.Seq

		if reply == nil {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v gotRPC nil, reply:%v", e.From, e.To, reply)
			return nil
		}
		if reply.Term > rf.currentTerm {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.Success {
			rf.setNextIndexAndMatchIndexAfterSnapshot(e.From)
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v gotRPC success, so set nextIndex %v, matchIndex %v", e.From, e.To, rf.nextIndex, rf.matchIndex)
		} else {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v gotRPC false, so do nothing and install again, %v", e.From, e.To)
			return nil
		}
	default:
		DPrintf(rf.me, fmt.Sprintf("ignore event:%v for raft:%v", e, rf.state))
	}
	return nil
}

func (rf *Raft) tickElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionElapsed++

	if rf.electionElapsed >= rf.randomizedElectionTimeout {
		DPrintf(rf.me, "tick election timeout !")

		if rf.state == StateCandidate || rf.state == StatePreCandidate {
			DPrintf(rf.me, "%s %v start election again, may encounter split vote at term %v !", rf.state, rf.me, rf.currentTerm)
			rf.becomeFollower(rf.currentTerm, None)
		}

		rf.electionElapsed = 0
		rf.send(Event{Type: EventPreHup, From: rf.me, Term: rf.currentTerm})
	}
}
func (rf *Raft) tickHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeatElapsed++
	rf.electionElapsed++

	if rf.state != StateLeader {
		return
	}

	if rf.heartbeatElapsed >= rf.heartbeatTimeout {
		DPrintf(rf.me, "tick heartbeat timeout !")
		rf.heartbeatElapsed = 0
		rf.send(Event{Type: EventBeat, From: rf.me, Term: rf.currentTerm})
	}
}
func (rf *Raft) reset(term int) {
	if rf.currentTerm != term {
		rf.currentTerm = term
		rf.votedFor = None
	}
	rf.lead = None
	rf.voteFrom = make(map[int]struct{})

	rf.electionElapsed = 0
	rf.heartbeatElapsed = 0
	rf.randomizedElectionTimeout = rf.electionTimeout + rand.Intn(rf.electionTimeout)
}

func (rf *Raft) send(e Event) {
	rf.eventCh <- e
}

func (rf *Raft) sendWait(e Event) {
	rf.eventCh <- e
	<-e.DoneC
}

func (rf *Raft) startRequestVote() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		peerIdx := idx
		lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
		args := &RequestVoteArgs{
			Seq:          rf.incSendRpcLatestSendSeq(peerIdx),
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}
		go func() {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v sendRPC %+v", rf.me, peerIdx, args)
			if ok := rf.peers[peerIdx].Call("Raft.RequestVote", args, reply); !ok {
				if !rf.killed() {
					DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v RPC not reply", rf.me, peerIdx)
				}
			} else {
				rf.send(Event{Type: EventVoteResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
			}
		}()
	}
}

func (rf *Raft) startPreRequestVote() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		peerIdx := idx
		lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
		args := &RequestVoteArgs{
			Seq: rf.incSendRpcLatestSendSeq(peerIdx),
			// 注意这里：preCandidate请求模拟term+1的情况，如果通过，后面becomeCandidate后，term+1自然也会成功，
			// 而follower那边对preRequestVote，不会覆盖自身term
			Term:         rf.currentTerm + 1,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}
		go func() {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v sendRPC %+v", rf.me, peerIdx, args)
			if ok := rf.peers[peerIdx].Call("Raft.RequestPreVote", args, reply); !ok {
				if !rf.killed() {
					DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v RPC not reply", rf.me, peerIdx)
				}
			} else {
				rf.send(Event{Type: EventPreVoteResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
			}
		}()
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// labrpc这里的调用到这里后，同步等待方法执行完，再返回reply
	rf.sendWait(Event{Type: EventVote, From: args.CandidateId, To: rf.me, Term: args.Term, Args: args, Reply: reply, DoneC: make(chan struct{})})
}

func (rf *Raft) RequestPreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.sendWait(Event{Type: EventPreVote, From: args.CandidateId, To: rf.me, Term: args.Term, Args: args, Reply: reply, DoneC: make(chan struct{})})
}

func (rf *Raft) startAppendEntries(isHeartbeat bool) {
	rf.setNextIndexAndMatchIndex(rf.me, rf.getLastLogIndex()-rf.nextIndex[rf.me]+1)

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		peerIdx := idx
		// 移出goroutine，这里存在并发情况，可能currentTerm受到其它RPC而增加了，并非最开始的term
		nextLogEntryIndex := rf.nextIndex[peerIdx]
		prevLogIndex := nextLogEntryIndex - 1
		if prevLogIndex < rf.getFirstLogIndex() {
			// install snapshot
			args := &InstallSnapshotArgs{
				Seq:               rf.incSendRpcLatestSendSeq(peerIdx),
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.getFirstLogIndex(),
				LastIncludedTerm:  rf.getFirstLogTerm(),
				Data:              rf.persister.ReadSnapshot(),
			}
			reply := &InstallSnapshotReply{}
			go func() {
				now := time.Now()
				DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v sendRPC %+v", rf.me, peerIdx, args)
				if ok := rf.peers[peerIdx].Call("Raft.InstallSnapshot", args, reply); !ok {
					if !rf.killed() {
						DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v RPC not reply", rf.me, peerIdx)
					}
				} else {
					duration := time.Now().Sub(now)
					rf.send(Event{Type: EventSnapResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
					TRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v consume time:%s", rf.me, peerIdx, duration)
				}
			}()
		} else {
			// append entries
			if !isHeartbeat {
				// inflight control
				rpcSeqStatus := rf.sendRpcLatestSeq[peerIdx]
				inflightCnt := rpcSeqStatus.SendSeq - rpcSeqStatus.RecvSeq
				if inflightCnt >= maxInflightAppCnt {
					DPrintf(rf.me, "InflightAppControl[%d] SendSeq:%v - RecvSeq:%v = inflightCnt:%v >= %d", peerIdx, rpcSeqStatus.SendSeq, rpcSeqStatus.RecvSeq, inflightCnt, maxInflightAppCnt)
					continue
				}
			}
			entries := make([]LogEntry, 0)
			if nextLogEntryIndex <= rf.getLastLogIndex() {
				entries = rf.getEntriesToEnd(nextLogEntryIndex)
			}
			args := &AppendEntriesArgs{
				Seq:          rf.incSendRpcLatestSendSeq(peerIdx),
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextLogEntryIndex - 1,
				PrevLogTerm:  rf.getLogEntry(nextLogEntryIndex - 1).Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			go func() {
				now := time.Now()
				DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v sendRPC %+v", rf.me, peerIdx, args)
				if ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply); !ok {
					if !rf.killed() {
						DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v RPC not reply", rf.me, peerIdx)
					}
				} else {
					duration := time.Now().Sub(now)
					rf.send(Event{Type: EventAppResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
					TRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v consume time:%s", rf.me, peerIdx, duration)
				}
			}()
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	now := time.Now()
	rf.sendWait(Event{Type: EventSnap, From: args.LeaderId, To: rf.me, Term: args.Term, Args: args, Reply: reply, DoneC: make(chan struct{})})
	duration := time.Now().Sub(now)
	TRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v consume time:%s", rf.me, args.LeaderId, duration)
}

func (rf *Raft) setNextIndexAndMatchIndexAfterSnapshot(peerIdx int) {
	rf.nextIndex[peerIdx] = rf.getFirstLogIndex() + 1
	rf.matchIndex[peerIdx] = rf.nextIndex[peerIdx] - 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now()
	rf.sendWait(Event{Type: EventApp, From: args.LeaderId, To: rf.me, Term: args.Term, Args: args, Reply: reply, DoneC: make(chan struct{})})
	duration := time.Now().Sub(now)
	TRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v consume time:%s", rf.me, args.LeaderId, duration)
}

func (rf *Raft) getLogEntryIndex(logIndex int) int {
	firstIndex := rf.getFirstLogEntry().Index
	lastIndex := rf.getLastLogEntry().Index
	if logIndex < firstIndex {
		DPrintf(rf.me, "GetPrevIndexAfterSnapshot getIndex:%v afterSnapshotFirstIndex:%v", logIndex, firstIndex)
		return 0
	}
	if logIndex > lastIndex {
		panic(fmt.Sprintf("Raft:%v got invalid logIndex:%v firstIndex:%v lastIndex:%v",
			rf.me, logIndex, firstIndex, lastIndex))
	}
	return logIndex - firstIndex
}

func (rf *Raft) getLogEntry(logIndex int) LogEntry {
	return rf.log[rf.getLogEntryIndex(logIndex)]
}

func (rf *Raft) getEntriesToEnd(startIdx int) []LogEntry {
	// [startIdx, endIdx)
	orig := rf.log[rf.getLogEntryIndex(startIdx):]
	x := make([]LogEntry, len(orig))
	copy(x, orig)
	return x
}

func (rf *Raft) getLastLogEntry() LogEntry {
	if len(rf.log) < 1 {
		panic(fmt.Sprintf("Raft:%v got empty log", rf.me))
	}
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) getLastLogIndex() int {
	return rf.getLastLogEntry().Index
}

func (rf *Raft) getLastLogIndexTerm() (int, int) {
	lastLogEntry := rf.getLastLogEntry()
	return lastLogEntry.Index, lastLogEntry.Term
}

func (rf *Raft) getFirstLogEntry() LogEntry {
	if len(rf.log) < 1 {
		panic(fmt.Sprintf("Raft:%v got empty log", rf.me))
	}
	return rf.log[0]
}

func (rf *Raft) getFirstLogIndex() int {
	return rf.getFirstLogEntry().Index
}

func (rf *Raft) getFirstLogTerm() int {
	return rf.getFirstLogEntry().Term
}

func (rf *Raft) getFirstIndexOfTerm(foundTerm int) int {
	for i := 0; i < len(rf.log)-1; i++ {
		logEntry := rf.log[i]
		if logEntry.Term == foundTerm {
			//DPrintf(rf.me, "foundTerm:%v, returnIndex:%v", foundTerm, logEntry.Index)
			return logEntry.Index
		}
	}
	firstIndex := rf.getFirstLogEntry().Index
	DPrintf(rf.me, "not found first index of term %v, so return firstIndex+1=%v", foundTerm, firstIndex+1)
	return firstIndex + 1
}

func (rf *Raft) setNextIndexAndMatchIndex(peerIdx int, sentEntriesLen int) {
	// 防止leader重复发送append RPC导致 nextIndex数组越界
	rf.nextIndex[peerIdx] = min(rf.nextIndex[peerIdx]+sentEntriesLen, rf.getLastLogIndex()+1)
	rf.matchIndex[peerIdx] = rf.nextIndex[peerIdx] - 1
}

//If there exists an N such that N > commitIndex, a majority
//of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
func (rf *Raft) calcCommitIndex() int {
	majorityCount := len(rf.peers)/2 + 1

	for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
		replicatedCnt := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N && rf.getLogEntry(N).Term == rf.currentTerm {
				replicatedCnt++
			}
			if replicatedCnt >= majorityCount {
				return N
			}
		}
	}

	return rf.commitIndex
}

func (rf *Raft) incSendRpcLatestSendSeq(peerIdx int) uint32 {
	rf.sendRpcLatestSeq[peerIdx].SendSeq++
	return rf.sendRpcLatestSeq[peerIdx].SendSeq
}

func (rf *Raft) setRecvRpcLatestSeq(peerIdx int, recvSeq uint32) {
	rf.recvRpcLatestSeq[peerIdx] = recvSeq
}

func (rf *Raft) asyncApplier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			// 这里取反，也就是说，只有当rf.lastApplied < rf.commitIndex时，才执行到下一步
		}
		commitIndex := rf.commitIndex
		deltaCount := commitIndex - rf.lastApplied
		DPrintf(rf.me, "%s will apply %v - %v = delta %v", rf.state, commitIndex, rf.lastApplied, deltaCount)
		needAppliedEntries := rf.getEntries(rf.lastApplied+1, commitIndex)
		rf.mu.Unlock()
		for _, entry := range needAppliedEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		rf.lastApplied += deltaCount
		rf.mu.Unlock()
	}
}

func (rf *Raft) asyncSnapshoter() {
	for {
		select {
		case msg := <-rf.asyncSnapshotCh:
			rf.applyCh <- msg
		case <-rf.killCh:
			return
		}
	}
}

func (rf *Raft) getEntries(startIdx, endIdx int) []LogEntry {
	// [startIdx, endIdx)
	orig := rf.log[rf.getLogEntryIndex(startIdx) : rf.getLogEntryIndex(endIdx)+1]
	x := make([]LogEntry, len(orig))
	copy(x, orig)
	return x
}

func (rf *Raft) initStates(peersNum int, persister *Persister) {
	// initialize from state persisted before a crash
	// including: currentTerm / votedFor / log
	raftState := persister.ReadRaftState()

	if raftState == nil || len(raftState) < 1 {
		rf.currentTerm = 0
		rf.votedFor = None
		rf.sendRpcLatestSeq = make(map[int]*RpcSeqStatus)
		for i := 0; i < peersNum; i++ {
			rf.sendRpcLatestSeq[i] = &RpcSeqStatus{}
		}
		rf.recvRpcLatestSeq = make(map[int]uint32)
		for i := 0; i < peersNum; i++ {
			rf.recvRpcLatestSeq[i] = 0
		}
		rf.log = make([]LogEntry, 0)
		rf.log = append(rf.log, LogEntry{nil, 0, 0})
	} else {
		rf.readPersist(raftState)
		DPrintf(rf.me, "Raft already recover")
	}

	firstLog := rf.log[0]
	// reset commitIndex / lastApplied
	rf.commitIndex = firstLog.Index
	rf.lastApplied = firstLog.Index
	// reset nextIndex / matchIndex
	rf.nextIndex = make([]int, peersNum)
	rf.matchIndex = make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	for i := 0; i < peersNum; i++ {
		rf.matchIndex[i] = 0
	}
	rf.replicatorTrigger = make(chan struct{}, 100)

	DPrintf(rf.me, "Raft init success, Last3Logs:%v commitIndex:%v lastApplied:%v nextIndex:%v sendRpcLatestSeq:%v recvRpcLatestSeq:%v",
		debugLast3Logs(rf.log), rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.sendRpcLatestSeq, rf.recvRpcLatestSeq)
}

func (rf *Raft) initInternalUsed() {
	rf.electionElapsed = 0
	rf.heartbeatElapsed = 0
	rf.electionTimeout = electionTimeout
	rf.heartbeatTimeout = heartbeatsTimeout
	rf.randomizedElectionTimeout = 0

	rf.killCh = make(chan struct{})
}

func newRaft(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg, eventCh chan Event) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = StateFollower

	rf.initStates(len(peers), persister)
	rf.initInternalUsed()

	rf.eventCh = eventCh
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.asyncSnapshotCh = make(chan ApplyMsg, 100)
	go rf.asyncApplier()
	go rf.asyncSnapshoter()
	go rf.replicator()

	return rf
}
