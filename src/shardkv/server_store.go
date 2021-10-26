package shardkv

import "fmt"

type ShardStore map[int]*Shard

type ShardStatus uint8

const (
	// 正常服务
	ShardServing ShardStatus = iota
	// 将要从 别的server pull该shard -> 更新为ShardNotifyPeerGidGC
	ShardPulling
	// 提供给 其它server pull该shard -> 更新为ShardNotServing
	ShardBePulling
	// pull完了 需要对方gid gc数据 -> 更新为ShardServing
	ShardNotifyPeerGidGC
	// bePulling完了 标志这个shard不在提供服务
	ShardNotServing
)

func (status ShardStatus) String() string {
	switch status {
	case ShardServing:
		return "ShardServing"
	case ShardPulling:
		return "ShardPulling"
	case ShardBePulling:
		return "ShardBePulling"
	case ShardNotifyPeerGidGC:
		return "ShardNotifyPeerGidGC"
	case ShardNotServing:
		return "ShardNotServing"
	}
	panic(fmt.Sprintf("unexpected ShardStatus %d", status))
}

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

func (shard *Shard) String() string {
	return fmt.Sprintf("{%s}", shard.Status)
}

func NewShard() *Shard {
	return &Shard{make(map[string]string), ShardServing}
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

func (shard *Shard) clone() map[string]string {
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

func (kv *ShardKV) canServe(shardID int) bool {
	return kv.currentConfig.Shards[shardID] == kv.gid &&
		// 要么是serving 要么是已经pull完了在通知对方peerGID gc
		(kv.shardStore[shardID].Status == ShardServing || kv.shardStore[shardID].Status == ShardNotifyPeerGidGC)
}

func (kv *ShardKV) GetShardsData(args *ShardsOpArgs, reply *ShardsOpReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Status = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	DPrintf(kv.gid, kv.me, "GetShardsData process args:%v", args)
	// 请求来的configNum比我大，说明我还未更新成最新的config，无法对外提供shard data
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Status = ErrNotReady
		return
	}

	reply.Shards = make(map[int]map[string]string)
	for _, shardID := range args.ShardIDs {
		reply.Shards[shardID] = kv.shardStore[shardID].clone()
	}

	reply.LastOperations = make(map[int64]LastOperation)
	for clientID, operation := range kv.sessions {
		reply.LastOperations[clientID] = operation
	}

	reply.ConfigNum, reply.Status = args.ConfigNum, OK
	return
}

func (kv *ShardKV) DeleteShardsData(args *ShardsOpArgs, reply *ShardsOpReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Status = ErrWrongLeader
		return
	}
	DPrintf(kv.gid, kv.me, "DeleteShardsData process args:%v", args)

	kv.mu.RLock()
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Status = OK
		DPrintf(kv.gid, kv.me, "DeleteShardsData alreadyProcess args:%v", args)
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	cmdReply := &CmdReply{}
	// 这里的CmdDeleteShards目的：删除 处在ShardBePulling状态的store
	kv.StartCmdAndWait(Command{CmdDeleteShards, *args}, cmdReply)
	reply.Status = cmdReply.Status
}
