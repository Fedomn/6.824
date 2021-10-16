package raft

import "fmt"

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

func (rf *Raft) getEntries(startIdx, endIdx int) []LogEntry {
	// [startIdx, endIdx]
	orig := rf.log[rf.getLogEntryIndex(startIdx) : rf.getLogEntryIndex(endIdx)+1]
	x := make([]LogEntry, len(orig))
	copy(x, orig)
	return x
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

func (rf *Raft) setNextIndexAndMatchIndexDirectly(peerIdx int, nextIndex int) {
	// 防止leader重复发送append RPC导致 nextIndex数组越界
	rf.nextIndex[peerIdx] = min(nextIndex, rf.getLastLogIndex()+1)
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
