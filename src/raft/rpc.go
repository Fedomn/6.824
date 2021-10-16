package raft

import "fmt"

const tickTimeout = 100 // raft internal tick is 100ms

// test中要求1s内完成election，因此减少这里的electionTimeout，尽量保证1s内多trigger几次election
const electionTimeout = 6 // random range: [electionTimeout, 2*electionTimeout] * tickTimeout

// follower处理的event的时间必须 < heartbeatsTimeout，否则下次heartbeat导致rpcSeq增加，leader又会abort old rpc
const heartbeatsTimeout = 3 // 3*tickTimeout

// 防止leader不断的start command，造成rpcSeq不断增加，会一直abort旧的seq response
// 导致集群中落后的raft会一直追不上leader
const maxInflightAppCnt = 10

type EventType int32

const (
	EventHup EventType = iota
	EventBeat
	EventReplicate
	EventVote
	EventVoteResp
	EventApp
	EventAppResp
	EventSnap
	EventSnapResp
	EventPreHup
	EventPreVote
	EventPreVoteResp
)

func (r EventType) String() string {
	switch r {
	case EventHup:
		return "EventHup"
	case EventBeat:
		return "EventBeat"
	case EventReplicate:
		return "EventReplicate"
	case EventVote:
		return "EventVote"
	case EventVoteResp:
		return "EventVoteResp"
	case EventApp:
		return "EventApp"
	case EventAppResp:
		return "EventAppResp"
	case EventSnap:
		return "EventSnap"
	case EventSnapResp:
		return "EventSnapResp"
	case EventPreHup:
		return "EventPreHup"
	case EventPreVote:
		return "EventPreVote"
	case EventPreVoteResp:
		return "EventPreVoteResp"
	default:
		return "unknown"
	}
}

type Event struct {
	Type EventType
	From int
	To   int
	Term int
	// need convert to corresponding struct according to eventType
	Args  interface{}
	Reply interface{}
	DoneC chan struct{}
}

func (e Event) String() string {
	return fmt.Sprintf("{Type:%v From:%v To:%v Term:%v}", e.Type, e.From, e.To, e.Term)
}

type RequestVoteArgs struct {
	Seq          uint32 // rpc sequence
	Term         int    // current candidate's term
	CandidateId  int    // current candidate id
	LastLogIndex int    // current candidate logs last log entry index
	LastLogTerm  int    // current candidate logs last log entry term
}

type RequestVoteReply struct {
	Term        int  // replied server's term
	VoteGranted bool // true means replied server agree
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Seq          uint32     // rpc sequence
	Term         int        // leader's term
	LeaderId     int        // leader id
	PrevLogIndex int        // index of log entry preceding the following entries
	PrevLogTerm  int        // term of pervLogIndex log entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader commit index
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Seq:%v Term:%v LeaderId:%v PrevLogIndex:%v PrevLogTerm:%v LeaderCommit:%v Last3Logs:%v}",
		a.Seq, a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, debugLast3Logs(a.Entries))
}

func debugLast3Logs(entries []LogEntry) []LogEntry {
	if len(entries) <= 3 {
		return entries
	} else {
		return entries[len(entries)-3:]
	}
}

type AppendEntriesReply struct {
	Term          int  // replied server's term
	Success       bool // ture means follower match consistency check use PrevLogIndex and PrevLogTerm
	ConflictIndex int
	NextIndex     int
}

type InstallSnapshotArgs struct {
	Seq               uint32 // rpc sequence
	Term              int    // leader's term
	LeaderId          int    // leader id
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

func (i InstallSnapshotArgs) String() string {
	return fmt.Sprintf("{Seq:%v Term:%v LeaderId:%v LastIncludedIndex:%v LastIncludedTerm:%v}",
		i.Seq, i.Term, i.LeaderId, i.LastIncludedIndex, i.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term    int  // replied server's term
	Success bool // ture means follower match consistency check use PrevLogIndex and PrevLogTerm
}

// None is a placeholder node ID used when there is no leader.
const None int = -1

type StateType int

const (
	StateFollower StateType = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

func (r StateType) String() string {
	switch r {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StatePreCandidate:
		return "pre-candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}
