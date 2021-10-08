package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOutdated    = "ErrOutdated"
)

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

func (o OpType) String() string {
	switch o {
	case OpGet:
		return "OpGet"
	case OpPut:
		return "OpPut"
	case OpAppend:
		return "OpAppend"
	default:
		return "unknown"
	}
}

type CommandArgs struct {
	OpType      OpType
	Key         string
	Value       string
	ClientId    int64
	SequenceNum int64
}

func (ca CommandArgs) clone() *CommandArgs {
	return &CommandArgs{
		OpType:      ca.OpType,
		Key:         ca.Key,
		Value:       ca.Value,
		ClientId:    ca.ClientId,
		SequenceNum: ca.SequenceNum,
	}
}

type CommandReply struct {
	Status     string
	Response   string
	LeaderHint int
}

func (cr CommandReply) clone() *CommandReply {
	return &CommandReply{
		Status:     cr.Status,
		Response:   cr.Response,
		LeaderHint: cr.LeaderHint,
	}
}
