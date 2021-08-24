package raft

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
	return rf.log[startIdx:]
}

func (rf *Raft) getEntriesFromStartTo(endIdx int) []LogEntry {
	return rf.log[:endIdx+1]
}

func (rf *Raft) setStatusWithLock(status raftStatus) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = status
	DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v", rf.me, status, rf.currentTerm)
}

func (rf *Raft) setLeaderWithLock() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = rf.commitIndex
	}
	// clean leader uncommitted log entries
	rf.log = rf.log[:rf.commitIndex+1]

	rf.status = leader
	DPrintf(rf.me, "Raft %v convert to %s, currentTerm:%v, log:%v, commitIndex:%v, lastApplied:%v, nextIndex:%v, matchIndex:%v",
		rf.me, leader, rf.currentTerm, rf.log, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
}

func (rf *Raft) setNextIndexAndMatchIndex(peerIdx int, sentEntriesLen int) {
	rf.nextIndex[peerIdx] = rf.nextIndex[peerIdx] + sentEntriesLen
	rf.matchIndex[peerIdx] = rf.nextIndex[peerIdx] - 1
}

func (rf *Raft) getStatusWithLock() raftStatus {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status
}

// 只看committed的log
func (rf *Raft) getLastCommittedLogIndexTerm() (int, int) {
	defer func() {
		if err := recover(); err != nil {
			DPrintf(rf.me, "Raft %d, log: %v, commitIndex: %d, nextIndex: %v\n", rf.me, rf.log, rf.commitIndex, rf.nextIndex)
			panic(err)
		}
	}()

	if len(rf.log) <= rf.commitIndex {
		DPrintf(rf.me, "bug: %v, %v", rf.log, rf.commitIndex)
	}
	return rf.commitIndex, rf.log[rf.commitIndex].Term
}

func (rf *Raft) isLeaderWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == leader
}

func (rf *Raft) isCandidateWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == candidate
}

func (rf *Raft) isFollowerWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == follower
}

func (rf *Raft) isHeartbeat(args *AppendEntriesArgs) bool {
	return len(args.Entries) == 0
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
