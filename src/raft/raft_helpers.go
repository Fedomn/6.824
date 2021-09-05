package raft

import "fmt"

// None is a placeholder node ID used when there is no leader.
const None int = 0

type StateType int

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

func (r StateType) String() string {
	switch r {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

func (rf *Raft) getCurrentTermWithLock() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLogEntry(logIndex int) LogEntry {
	return rf.log[logIndex]
}

func (rf *Raft) getLastLogIndexTerm() (int, int) {
	lastIndex := rf.getLastLogIndex()
	return lastIndex, rf.getLogEntry(lastIndex).Term
}

// [startIdx, endIdx)
func (rf *Raft) getEntriesToEnd(startIdx int) []LogEntry {
	defer func() {
		if err := recover(); err != nil {
			DPrintf(rf.me, "Raft %d, log: %v, commitIndex: %d, nextIndex: %v\n", rf.me, rf.log, rf.commitIndex, rf.nextIndex)
			panic(err)
		}
	}()
	// 注意这里需要clone一份出来，因为它的return值 会直接作为AppendEntriesArgs的Entries
	// 如果不做clone，会出现data race
	// write 在AppendEntries里的rf.log切片
	// read 在labrpc里的labgob encode读取entries
	return rf.cloneLogEntries(rf.log[startIdx:])
}

func (rf *Raft) cloneLogEntries(orig []LogEntry) []LogEntry {
	x := make([]LogEntry, len(orig))
	copy(x, orig)
	return x
}

func (rf *Raft) getEntriesFromStartTo(endIdx int) []LogEntry {
	return rf.log[:endIdx+1]
}

func (rf *Raft) setStatusWithLock(status StateType) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = status
	DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v", rf.me, status, rf.currentTerm)
}

func (rf *Raft) setNextIndexAndMatchIndex(peerIdx int, sentEntriesLen int) {
	// 防止leader重复发送append RPC导致 nextIndex数组越界
	rf.nextIndex[peerIdx] = min(rf.nextIndex[peerIdx]+sentEntriesLen, rf.getLastLogIndex()+1)
	rf.matchIndex[peerIdx] = rf.nextIndex[peerIdx] - 1
}

func (rf *Raft) getStatusWithLock() StateType {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) isLeaderWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == StateLeader
}

func (rf *Raft) isCandidateWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == StateCandidate
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

func (rf *Raft) getFirstIndexOfTerm(foundTerm int) int {
	for i := 0; i < len(rf.log)-1; i++ {
		if rf.log[i].Term == foundTerm {
			DPrintf(rf.me, "foundTerm:%v, got:%v", foundTerm, i)
			return i
		}
	}
	panic(fmt.Sprintf("not found first index of term %v", foundTerm))
}

func (rf *Raft) becomeFollower(term int, lead int) {
	rf.reset(term)
	rf.state = StateFollower

	rf.resetElectionSignal <- struct{}{}
	rf.resetAppendEntriesSignal <- struct{}{}
	DPrintf(rf.me, "Raft %v became follower at term %v", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeCandidate() {
	if rf.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	rf.reset(rf.currentTerm + 1)
	rf.state = StateCandidate
	rf.votedFor = rf.me

	// reset 计数器 include candidate itself
	rf.requestVoteCnt = 1
	rf.requestVoteGrantedCnt = 1
	DPrintf(rf.me, "Raft %v became candidate at term %v", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeLeader() {
	if rf.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	rf.reset(rf.currentTerm)
	rf.state = StateLeader

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}

	// 不应该clean leader uncommitted log entries
	// 因为当选的leader还需要完成上一轮leader的replication任务

	// reset计数器
	rf.appendEntriesCnt = 1
	rf.appendEntriesSuccessCnt = 1

	DPrintf(rf.me, "Raft %v became leader at term %v, "+
		"commitIndex:%v, lastApplied:%v, nextIndex:%v, matchIndex:%v, log:%v",
		rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, rf.log)
}

func (rf *Raft) isEncounterSplitVoteWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	majorityCount := len(rf.peers)/2 + 1
	return rf.requestVoteCnt == len(rf.peers) && rf.requestVoteGrantedCnt < majorityCount
}

func (rf *Raft) isGotMajorityVoteWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	majorityCount := len(rf.peers)/2 + 1
	return rf.requestVoteGrantedCnt >= majorityCount && rf.state == StateCandidate
}

func (rf *Raft) isEncounterPartitionWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	majorityCount := len(rf.peers)/2 + 1
	return rf.appendEntriesCnt == len(rf.peers) && rf.appendEntriesSuccessCnt < majorityCount
}

func (rf *Raft) isGotMajorityAppendSuccessWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	majorityCount := len(rf.peers)/2 + 1
	return rf.appendEntriesSuccessCnt >= majorityCount
}

func (rf *Raft) reset(term int) {
	if rf.currentTerm != term {
		rf.currentTerm = term
		rf.votedFor = None
	}
}

func (rf *Raft) safe(fun func()) {
	rf.mu.Lock()
	fun()
	rf.mu.Unlock()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
