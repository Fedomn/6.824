package raft

import (
	"6.824/labrpc"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (a ApplyMsg) String() string {
	if a.CommandValid {
		return fmt.Sprintf("Command:{<%d,%d> %s}", a.CommandTerm, a.CommandIndex, a.Command)
	} else if a.SnapshotValid {
		return fmt.Sprintf("Snapshot:{<%d,%d>}", a.SnapshotTerm, a.SnapshotIndex)
	} else {
		return "InvalidApplyMsg"
	}
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

	gLog *log.Logger
}

type RpcSeqStatus struct {
	SendSeq uint32
	RecvSeq uint32
}

func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLastLogEntry().Term == rf.currentTerm
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
				rf.DPrintf(rf.me, "Leader proposeT immediately")
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

func (rf *Raft) send(e Event) {
	rf.eventCh <- e
}

func (rf *Raft) sendWait(e Event) {
	rf.eventCh <- e
	<-e.DoneC
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
		needAppliedEntries := rf.getEntries(rf.lastApplied+1, commitIndex)
		rf.DPrintf(rf.me, "%s will apply %v - %v = delta %v, entries:%v", rf.state, commitIndex, rf.lastApplied, deltaCount, debugLast3Logs(needAppliedEntries))
		rf.mu.Unlock()
		for _, entry := range needAppliedEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		// 这里会snapshot设置lastApplied出现并发问题，如
		// snapshot idx=800在前，apply idx=797在后，因此以最新的rf.lastApplied再重新设置。
		// snapshot idx=700在前，apply idx=701在后，这是正常流程用当前apply的commitIndex来设置。
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
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
		rf.DPrintf(rf.me, "Raft already recover")
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

	rf.DPrintf(rf.me, "Raft init success, Last3Logs:%v commitIndex:%v lastApplied:%v nextIndex:%v sendRpcLatestSeq:%v recvRpcLatestSeq:%v",
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

func newRaft(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg, eventCh chan Event, glog *log.Logger) *Raft {
	rf := &Raft{gLog: initGlog(glog)}
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
