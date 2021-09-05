package raft

import "fmt"

// election timeout range special for test, because test require heartbeat minimum 100ms
var electionTimeoutRange = []int{300, 600}

const heartbeatsTimeout = 120

type RequestVoteArgs struct {
	Term         int // current candidate's term
	CandidateId  int // current candidate id
	LastLogIndex int // current candidate logs last log entry index
	LastLogTerm  int // current candidate logs last log entry term
}

type RequestVoteReply struct {
	Term        int  // replied server's term
	VoteGranted bool // true means replied server agree
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader id
	PrevLogIndex int        // index of log entry preceding the following entries
	PrevLogTerm  int        // term of pervLogIndex log entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader commit index
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v LeaderId:%v PrevLogIndex:%v PrevLogTerm:%v LeaderCommit:%v}",
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit)
}

type AppendEntriesReply struct {
	Term          int  // replied server's term
	Success       bool // ture means follower match consistency check use PrevLogIndex and PrevLogTerm
	ConflictIndex int
}
