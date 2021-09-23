package v2

import "fmt"

const tickTimeout = 100 // raft internal tick is 100ms
// test中要求1s内完成election，因此减少这里的electionTimeout，尽量保证1s内多trigger几次election
const electionTimeout = 5   // random range: [electionTimeout, 2*electionTimeout] * tickTimeout
const heartbeatsTimeout = 1 // 1*tickTimeout

type EventType int32

const (
	EventHup EventType = iota
	EventBeat
	EventVote
	EventVoteResp
	EventApp
	EventAppResp
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
	case EventVote:
		return "EventVote"
	case EventVoteResp:
		return "EventVoteResp"
	case EventApp:
		return "EventApp"
	case EventAppResp:
		return "EventAppResp"
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
	//return fmt.Sprintf("{Term:%v LeaderId:%v PrevLogIndex:%v PrevLogTerm:%v LeaderCommit:%v Entries:%v}",
	//	a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, a.Entries)
	return fmt.Sprintf("{Term:%v LeaderId:%v PrevLogIndex:%v PrevLogTerm:%v LeaderCommit:%v}",
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit)
}

type AppendEntriesReply struct {
	Term          int  // replied server's term
	Success       bool // ture means follower match consistency check use PrevLogIndex and PrevLogTerm
	ConflictIndex int
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
