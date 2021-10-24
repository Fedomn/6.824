package shardctrler

import "fmt"

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOutdated    = "ErrOutdated"
)

type CommandArgs struct {
	OpType      OpType
	ClientId    int64
	SequenceNum int64

	// JoinArgs
	Servers map[int][]string // new GID -> servers mappings

	// LeaveArgs
	GIDs []int

	// MoveArgs
	Shard int
	GID   int

	// QueryArgs
	Num int // desired config number
}

func (ca CommandArgs) String() string {
	switch ca.OpType {
	case OpJoin:
		return fmt.Sprintf("[%d:%d] %s<Servers:%v>", ca.ClientId, ca.SequenceNum, ca.OpType, ca.Servers)
	case OpLeave:
		return fmt.Sprintf("[%d:%d] %s<GIDs:%v>", ca.ClientId, ca.SequenceNum, ca.OpType, ca.GIDs)
	case OpMove:
		return fmt.Sprintf("[%d:%d] %s<Shard:%v GID:%v>", ca.ClientId, ca.SequenceNum, ca.OpType, ca.Shard, ca.GID)
	case OpQuery:
		return fmt.Sprintf("[%d:%d] %s<Num:%v>", ca.ClientId, ca.SequenceNum, ca.OpType, ca.Num)
	}
	panic(fmt.Sprintf("unexpected OpType %d", ca.OpType))
}

func (ca CommandArgs) clone() *CommandArgs {
	return &CommandArgs{
		OpType:      ca.OpType,
		ClientId:    ca.ClientId,
		SequenceNum: ca.SequenceNum,
		Servers:     ca.Servers,
		GIDs:        ca.GIDs,
		Shard:       ca.Shard,
		GID:         ca.GID,
		Num:         ca.Num,
	}
}

type CommandReply struct {
	Status string

	// QueryReply
	Config Config
}

func (cr CommandReply) String() string {
	return fmt.Sprintf("{Status:%s,Config:%s}", cr.Status, cr.Config)
}

type OpType int

const (
	OpJoin OpType = iota
	OpLeave
	OpMove
	OpQuery
)

func (o OpType) String() string {
	switch o {
	case OpJoin:
		return "OpJoin"
	case OpLeave:
		return "OpLeave"
	case OpMove:
		return "OpMove"
	case OpQuery:
		return "OpQuery"
	}
	panic(fmt.Sprintf("unexpected OpType %d", o))
}
