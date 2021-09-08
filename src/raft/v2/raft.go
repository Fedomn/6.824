package v2

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
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

	waitRequestVoteDone   map[int]chan struct{}
	waitAppendEntriesDone map[int]chan struct{}

	killCh chan struct{}

	// for tester
	applyCh chan ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil {
		panic("persist encounter error")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	if d.Decode(&x) != nil ||
		d.Decode(&y) != nil ||
		d.Decode(&z) != nil {
		panic("readPersist encounter error")
	} else {
		rf.currentTerm = x
		rf.votedFor = y
		rf.log = z
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
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

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	index = rf.getLastLogIndex()
	term = rf.currentTerm
	return
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
	DPrintf(rf.me, "Raft %v became follower at term %v", rf.me, rf.currentTerm)
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
}
func (rf *Raft) becomeLeader() {
	if rf.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	rf.reset(rf.currentTerm)
	rf.state = StateLeader
	rf.tick = rf.tickHeartbeat
	rf.step = stepLeader

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}

	DPrintf(rf.me, "Raft %v became leader at term %v, "+
		"commitIndex:%v, lastApplied:%v, nextIndex:%v, matchIndex:%v, log:%v",
		rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, debugLog(rf.log))
}

func (rf *Raft) Step(e Event) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch e.Type {
	case EventVote:
		args := e.Args.(*RequestVoteArgs)
		reply := e.Reply.(*RequestVoteReply)
		//DPrintf(rf.me, "Debug EventVote args:%+v reply:%+v", args, reply)
		switch {
		case args.Term < rf.currentTerm:
			DPrintf(rf.me, "RequestVote %v<-%v currentTerm %v > term %v, ignore lower term", rf.me, args.CandidateId, rf.currentTerm, args.Term)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		case args.Term == rf.currentTerm:
			DPrintf(rf.me, "RequestVote %v<-%v currentTerm %v == term %v, already votedFor %v", rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.votedFor)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		case args.Term > rf.currentTerm:
			originalCurrentTerm := rf.currentTerm
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm

			passCheck := true
			for loop := true; loop; loop = false {
				lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
				if args.LastLogTerm < lastLogTerm {
					passCheck = false
					DPrintf(rf.me, "RequestVote %v<-%v fail election restriction check about term. %v < %v", rf.me, args.CandidateId, args.LastLogTerm, lastLogTerm)
					break
				}

				if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
					passCheck = false
					DPrintf(rf.me, "RequestVote %v<-%v fail election restriction check about index when same term. %v < %v", rf.me, args.CandidateId, args.LastLogIndex, lastLogIndex)
					break
				}
			}
			if passCheck {
				TPrintf(rf.me, "RequestVote %v<-%v pass election restriction", rf.me, args.CandidateId)
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
			}

			if rf.state != StateFollower {
				DPrintf(rf.me, "RequestVote %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately",
					rf.me, args.CandidateId, rf.state, originalCurrentTerm, reply.Term)
				rf.becomeFollower(args.Term, None)
			}
		}
		if reply.VoteGranted {
			rf.electionElapsed = 0
		}
		rf.waitRequestVoteDone[args.CandidateId] <- struct{}{}
		return nil
	case EventApp:
		args := e.Args.(*AppendEntriesArgs)
		reply := e.Reply.(*AppendEntriesReply)
		//DPrintf(rf.me, "Debug EventApp args:%+v reply:%+v", args, reply)
		switch {
		case args.Term < rf.currentTerm:
			DPrintf(rf.me, "AppendEntries %v<-%v currentTerm %v > term %v, ignore lower term",
				rf.me, args.LeaderId, rf.currentTerm, args.Term)
			reply.Term = rf.currentTerm
			reply.Success = false
		case args.Term >= rf.currentTerm:
			if rf.state != StateFollower {
				DPrintf(rf.me, "AppendEntries %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately",
					rf.me, args.LeaderId, rf.state, rf.currentTerm, args.Term)
				rf.becomeFollower(args.Term, None)
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
					DPrintf(rf.me, "AppendEntries %v<-%v fail consistency for index. %v < %v, conflictIndex:%v",
						rf.me, args.LeaderId, lastLogIndex, args.PrevLogIndex, reply.ConflictIndex)
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
					DPrintf(rf.me, "AppendEntries %v<-%v fail consistency for term. %v != %v, conflictIndex:%v",
						rf.me, args.LeaderId, matchedIndexLogTerm, args.PrevLogTerm, reply.ConflictIndex)
					break
				}
			}
			if passCheck {
				rf.currentTerm = args.Term
				reply.Term = rf.currentTerm

				TPrintf(rf.me, "AppendEntries %v<-%v pass consistency check", rf.me, args.LeaderId)
				reply.Success = true

				newLog := make([]LogEntry, 0)
				for _, entry := range args.Entries {
					logEntry := LogEntry{
						Command: entry.Command,
						Term:    entry.Term,
					}
					newLog = append(newLog, logEntry)
				}
				rf.log = append(rf.log[:args.PrevLogIndex+1], newLog...)

				// 如果leader commit index > follower 本地存储的 commit index，
				// 则更新 follower本地的 commitIndex = min(leaderCommit , 将要保存的logs中最后一个log entry index)
				if args.LeaderCommit > rf.commitIndex {
					// 存在一种情况，follower落后leader很多，这次appendEntries还未补全所有log，
					// 所以 这次follower的committedIndex为最后一个logIndex
					rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
					go rf.applyLogsWithLock()
				}
			} else {
				reply.Success = false
				reply.Term = rf.currentTerm
			}
		}
		if reply.Success {
			rf.electionElapsed = 0
		}
		rf.waitAppendEntriesDone[args.LeaderId] <- struct{}{}
		return nil
	default:
		return rf.step(rf, e)
	}
}
func stepFollower(rf *Raft, e Event) error {
	switch e.Type {
	case EventHup:
		rf.becomeCandidate()
		rf.send(e)
	default:
		DPrintf(rf.me, fmt.Sprintf("ignore event:%v for %v", e, rf.state))
	}
	return nil
}
func stepCandidate(rf *Raft, e Event) error {
	switch e.Type {
	case EventHup:
		rf.startRequestVote()
	case EventVoteResp:
		reply := e.Reply.(*RequestVoteReply)
		TPrintf(rf.me, "RequestVote %v->%v RPC got %+v", e.From, e.To, reply)
		if reply == nil {
			return nil
		}
		if reply.Term > rf.currentTerm {
			DPrintf(rf.me, "RequestVote %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.VoteGranted {
			rf.voteFrom[e.From] = struct{}{}
		}
		DPrintf(rf.me, "RequestVote %v->%v voteFrom %+v", e.From, e.To, rf.voteFrom)
		// got majority votes
		if len(rf.voteFrom) >= len(rf.peers)/2+1 {
			rf.becomeLeader()
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
		rf.startAppendEntries()
	case EventAppResp:
		args := e.Args.(*AppendEntriesArgs)
		reply := e.Reply.(*AppendEntriesReply)
		TPrintf(rf.me, "AppendEntries %v->%v RPC got %+v", e.From, e.To, reply)
		if reply == nil {
			return nil
		}
		if reply.Term > rf.currentTerm {
			DPrintf(rf.me, "AppendEntries %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.Success {
			if len(args.Entries) == 0 {
				TPrintf(rf.me, "AppendEntries %v->%v RPC got success, entries len: %v, so heartbeat will do nothing",
					e.From, e.To, len(args.Entries))
			} else {
				rf.setNextIndexAndMatchIndex(e.From, len(args.Entries))
				TPrintf(rf.me, "AppendEntries %v->%v RPC got success, entries len: %v, so had increased nextIndex %v, matchIndex %v",
					e.From, e.To, len(args.Entries), rf.nextIndex, rf.matchIndex)
			}
		} else {
			rf.nextIndex[e.From] = reply.ConflictIndex
			DPrintf(rf.me, "AppendEntries %v->%v RPC got false, so will decrease nextIndex and append again, %v",
				e.From, e.To, rf.nextIndex)
			return nil
		}

		// maybeCommit
		calcCommitIndex := rf.calcCommitIndex()
		if rf.commitIndex != calcCommitIndex {
			DPrintf(rf.me, "AppendEntries %v->%v maybe commit, before:%v after calc commitIndex:%v, matchIndex: %v",
				e.From, e.To, rf.commitIndex, calcCommitIndex, rf.matchIndex)
			rf.commitIndex = calcCommitIndex

			deltaLogsCount := rf.commitIndex - rf.lastApplied
			if deltaLogsCount > 0 {
				DPrintf(rf.me, "AppendEntries %v->%v will apply %v - %v = delta %v",
					e.From, e.To, rf.commitIndex, rf.lastApplied, deltaLogsCount)
				go rf.applyLogsWithLock()
			}
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

		if rf.state == StateCandidate {
			DPrintf(rf.me, "Candidate %v start election again, may encounter split vote at term %v !", rf.me, rf.currentTerm)
			rf.becomeFollower(rf.currentTerm, None)
		}

		rf.electionElapsed = 0
		rf.send(Event{Type: EventHup, From: rf.me, Term: rf.currentTerm})
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
	//TPrintf(rf.me, "Send Event: %+v", e)
}

func (rf *Raft) startRequestVote() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		peerIdx := idx
		lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}
		go func() {
			DPrintf(rf.me, "RequestVote %v->%v send RPC %+v", rf.me, peerIdx, args)
			if ok := rf.peers[peerIdx].Call("Raft.RequestVote", args, reply); !ok {
				if !rf.killed() {
					TPrintf(rf.me, "RequestVote %v->%v RPC not reply", rf.me, peerIdx)
				}
			} else {
				rf.send(Event{Type: EventVoteResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
			}
		}()
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// labrpc这里的调用到这里后，同步等待方法执行完，再返回reply
	rf.send(Event{Type: EventVote, From: args.CandidateId, To: rf.me, Term: args.Term, Args: args, Reply: reply})
	// 这里可能会被并发请求，导致同时等待waitRequestVoteDone，可能出现后来的RPC还未处理完，
	// 却被之前的RPC先释放了done
	<-rf.waitRequestVoteDone[args.CandidateId]
}

func (rf *Raft) startAppendEntries() {
	if rf.commitIndex == rf.getLastLogIndex() {
		// 没有uncommitted log entries，则append empty heartbeat
		TPrintf(rf.me, "AppendEntries no uncommitted log entries")
	} else {
		TPrintf(rf.me, "AppendEntries has uncommitted log entries")
	}
	rf.setNextIndexAndMatchIndex(rf.me, len(rf.getEntriesToEnd(rf.nextIndex[rf.me])))

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		peerIdx := idx
		// 移出goroutine，这里存在并发情况，可能currentTerm受到其它RPC而增加了，并非最开始的term
		nextLogEntryIndex := rf.nextIndex[peerIdx]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextLogEntryIndex - 1,
			PrevLogTerm:  rf.getLogEntry(nextLogEntryIndex - 1).Term,
			Entries:      rf.getEntriesToEnd(nextLogEntryIndex),
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		go func() {
			DPrintf(rf.me, "AppendEntries %v->%v send RPC %+v", rf.me, peerIdx, args)
			if ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply); !ok {
				if !rf.killed() {
					TPrintf(rf.me, "AppendEntries %v->%v RPC not reply", rf.me, peerIdx)
				}
			} else {
				rf.send(Event{Type: EventAppResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
			}
		}()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.send(Event{Type: EventApp, From: args.LeaderId, To: rf.me, Term: args.Term, Args: args, Reply: reply})
	<-rf.waitAppendEntriesDone[args.LeaderId]
}

func (rf *Raft) applyLogsWithLock() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.getLogEntry(i).Command,
			CommandIndex: i,
		}
		rf.lastApplied++
	}
	if rf.lastApplied+1 <= rf.commitIndex {
		DPrintf(rf.me, "Applied entries: %v~%v", rf.lastApplied+1, rf.commitIndex)
	}
	rf.persist()
}

func (rf *Raft) getLogEntry(logIndex int) LogEntry {
	return rf.log[logIndex]
}

func (rf *Raft) getEntriesToEnd(startIdx int) []LogEntry {
	// [startIdx, endIdx)
	orig := rf.log[startIdx:]
	x := make([]LogEntry, len(orig))
	copy(x, orig)
	return x
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogIndexTerm() (int, int) {
	lastIndex := rf.getLastLogIndex()
	return lastIndex, rf.getLogEntry(lastIndex).Term
}

func (rf *Raft) getFirstIndexOfTerm(foundTerm int) int {
	for i := 0; i < len(rf.log)-1; i++ {
		if rf.log[i].Term == foundTerm {
			DPrintf(rf.me, "foundTerm:%v, got:%v", foundTerm, i)
			return i
		}
	}
	panic(fmt.Sprintf("not found first index of term %v", foundTerm))
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
			if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				replicatedCnt++
			}
			if replicatedCnt >= majorityCount {
				return N
			}
		}
	}

	return rf.commitIndex
}

func newRaft(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg, eventCh chan Event) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = StateFollower
	rf.currentTerm = 0
	rf.votedFor = None
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{}) // first empty logEntry
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}

	rf.electionElapsed = 0
	rf.heartbeatElapsed = 0
	rf.electionTimeout = electionTimeout
	rf.heartbeatTimeout = heartbeatsTimeout
	rf.randomizedElectionTimeout = 0

	rf.eventCh = eventCh

	rf.applyCh = applyCh

	rf.waitRequestVoteDone = make(map[int]chan struct{})
	for i := 0; i < len(rf.peers); i++ {
		rf.waitRequestVoteDone[i] = make(chan struct{})
	}
	rf.waitAppendEntriesDone = make(map[int]chan struct{})
	for i := 0; i < len(rf.peers); i++ {
		rf.waitAppendEntriesDone[i] = make(chan struct{})
	}
	rf.killCh = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
