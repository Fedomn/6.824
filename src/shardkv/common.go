package shardkv

import (
	"fmt"
	"time"
)

const (
	ExecuteTimeout       = 500 * time.Millisecond
	MonitorConfigTimeout = 100 * time.Millisecond
	MonitorPullTimeout   = 50 * time.Millisecond
	MonitorGCTimeout     = 50 * time.Millisecond
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutdated    = "ErrOutdated"
	ErrTimeout     = "ErrTimeout"
	ErrNotReady    = "ErrNotReady"
)

type LastOperation struct {
	SequenceNum int64
	Reply       CmdReply
}

type CmdOpType int

const (
	CmdOpGet CmdOpType = iota
	CmdOpPut
	CmdOpAppend
)

func (o CmdOpType) String() string {
	switch o {
	case CmdOpGet:
		return "CmdOpGet"
	case CmdOpPut:
		return "CmdOpPut"
	case CmdOpAppend:
		return "CmdOpAppend"
	default:
		return "unknown"
	}
}

type CmdType int

const (
	CmdOp CmdType = iota
	CmdConfig
	CmdInsertShards
	CmdDeleteShards
)

func (cmd CmdType) String() string {
	switch cmd {
	case CmdOp:
		return "Operation"
	case CmdConfig:
		return "Configuration"
	case CmdInsertShards:
		return "InsertShards"
	case CmdDeleteShards:
		return "DeleteShards"
	default:
		return "unknown"
	}
}

type Command struct {
	CmdType CmdType
	CmdArgs interface{}
}

func (c Command) String() string {
	return fmt.Sprintf("%s<%s>", c.CmdType, c.CmdArgs)
}

// operation
type CmdOpArgs struct {
	OpType      CmdOpType
	Key         string
	Value       string
	ClientId    int64
	SequenceNum int64
}

func (ca CmdOpArgs) String() string {
	return fmt.Sprintf("[%d:%d] %s<%s,%s>", ca.ClientId, ca.SequenceNum, ca.OpType, ca.Key, ca.Value)
}

func (ca CmdOpArgs) clone() *CmdOpArgs {
	return &CmdOpArgs{
		OpType:      ca.OpType,
		Key:         ca.Key,
		Value:       ca.Value,
		ClientId:    ca.ClientId,
		SequenceNum: ca.SequenceNum,
	}
}

type CmdReply struct {
	Status     string
	Response   string
	LeaderHint int
}

func (cr *CmdReply) String() string {
	if len(cr.Response) > 5 {
		return fmt.Sprintf("%s ...%s", cr.Status, cr.Response[len(cr.Response)-5:])
	} else {
		return fmt.Sprintf("%s %s", cr.Status, cr.Response)
	}
}

func (cr CmdReply) clone() *CmdReply {
	return &CmdReply{
		Status:     cr.Status,
		Response:   cr.Response,
		LeaderHint: cr.LeaderHint,
	}
}

type ShardsOpArgs struct {
	ConfigNum int
	ShardIDs  []int
}

func (sa ShardsOpArgs) String() string {
	return fmt.Sprintf("{ConfigNum:%d,ShardIDs:%v}", sa.ConfigNum, sa.ShardIDs)
}

type ShardsOpReply struct {
	Status         string
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]LastOperation
}

func (sr ShardsOpReply) String() string {
	return fmt.Sprintf("{ConfigNum:%d,Status:%s,Shards:%v,LastOperations:%v}",
		sr.ConfigNum, sr.Status, sr.Shards, sr.LastOperations)
}
