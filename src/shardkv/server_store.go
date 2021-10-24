package shardkv

import "fmt"

type ShardStore map[int]Shard

type ShardStatus uint8

const (
	ShardServing ShardStatus = iota
	ShardPulling
	ShardBePulling
	ShardGCing
)

func (status ShardStatus) String() string {
	switch status {
	case ShardServing:
		return "ShardServing"
	case ShardPulling:
		return "ShardPulling"
	case ShardBePulling:
		return "ShardBePulling"
	case ShardGCing:
		return "ShardGCing"
	}
	panic(fmt.Sprintf("unexpected ShardStatus %d", status))
}

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

func (shard *Shard) Get(key string) (string, string) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) string {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key, value string) string {
	shard.KV[key] += value
	return OK
}

func (shard *Shard) deepCopy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range shard.KV {
		newShard[k] = v
	}
	return newShard
}

func (kv *ShardKV) applyToStore(op CmdOpArgs, shardId int) CmdReply {
	switch op.OpType {
	case CmdOpGet:
		return CmdReply{Response: kv.shardStore[shardId].KV[op.Key], Status: OK}
	case CmdOpPut:
		kv.shardStore[shardId].KV[op.Key] = op.Value
		return CmdReply{Response: kv.shardStore[shardId].KV[op.Key], Status: OK}
	case CmdOpAppend:
		kv.shardStore[shardId].KV[op.Key] += op.Value
		return CmdReply{Response: kv.shardStore[shardId].KV[op.Key], Status: OK}
	default:
		panic(fmt.Sprintf("invalid op: %v", op))
	}
}
