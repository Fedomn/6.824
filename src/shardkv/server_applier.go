package shardkv

import (
	"6.824/shardctrler"
	"fmt"
)

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			DPrintf(kv.gid, kv.me, "ShardKVServerApplier gotApplyMsg:%v", msg)
			switch {
			case msg.CommandValid:
				if msg.CommandIndex <= kv.lastApplied {
					DPrintf(kv.gid, kv.me, "ShardKVServerApplier discardOutdatedMsgIndex:%d lastApplied:%d", msg.CommandIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				cmd := msg.Command.(Command)
				reply := CmdReply{}

				switch cmd.CmdType {
				case CmdOp:
					op := cmd.CmdArgs.(CmdOpArgs)
					reply = kv.applyOp(op)
				case CmdConfig:
					latestConfig := cmd.CmdArgs.(shardctrler.Config)
					reply = kv.applyConfig(latestConfig)
				case CmdInsertShards:
					shardsOpReply := cmd.CmdArgs.(ShardsOpReply)
					reply = kv.applyInsertShards(shardsOpReply)
				case CmdDeleteShards:
					shardsOpArgs := cmd.CmdArgs.(ShardsOpArgs)
					reply = kv.applyDeleteShards(shardsOpArgs)
				case CmdNoop:
					reply = kv.applyNoop()
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader {
					if msg.CommandTerm <= currentTerm {
						if ch, ok := kv.notifyCh[msg.CommandIndex]; ok {
							ch <- reply
						} else {
							DPrintf(kv.gid, kv.me, "ShardKVServerApplier gotApplyMsgTimeout index:%d", msg.CommandIndex)
						}
					} else {
						DPrintf(kv.gid, kv.me, "ShardKVServerApplier lostLeadership")
					}
				}

				if kv.maxraftstate != -1 && kv.rfPersister.RaftStateSize() > kv.maxraftstate {
					beforeSize := kv.rfPersister.RaftStateSize()
					DPrintf(kv.gid, kv.me, "ShardKVServerApplier %d > %d willSnapshot shardStore:%v", beforeSize, kv.maxraftstate, kv.shardStore)
					kv.rf.Snapshot(msg.CommandIndex, kv.makeSnapshot())
					DPrintf(kv.gid, kv.me, "ShardKVServerApplier afterSnapshotKvSize:%d", kv.rfPersister.RaftStateSize())
				}
			case msg.SnapshotValid:
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.installSnapshot(msg.Snapshot)
					DPrintf(kv.gid, kv.me, "ShardKVServerApplier willInstallSnapshot:%v", kv.shardStore)
					kv.lastApplied = msg.SnapshotIndex
				}
			default:
				panic(fmt.Sprintf("ShardKVServerApplier Unexpected message %v", msg))
			}
			kv.mu.Unlock()
		}
	}
}

// CmdOp
func (kv *ShardKV) applyOp(op CmdOpArgs) CmdReply {
	shardId := key2shard(op.Key)
	if !kv.canServe(shardId) {
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier canNotServe shardId:%d status:%s", shardId, kv.shardStore[shardId].Status)
		return CmdReply{Status: ErrWrongGroup}
	}

	if kv.isOutdatedCommand(op.ClientId, op.SequenceNum) {
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier gotOutdatedCommand:[%d,%d]", op.ClientId, op.SequenceNum)
		kv.mu.RUnlock()
		return CmdReply{Status: ErrOutdated}
	}

	isGetOp := op.OpType == CmdOpGet
	if isDuplicated, lastReply := kv.getDuplicatedCommandReply(op.ClientId, op.SequenceNum); isDuplicated && isGetOp {
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier gotDuplicatedCommand:[%d,%d]", op.ClientId, op.SequenceNum)
		return lastReply
	} else {
		reply := kv.applyToStore(op, shardId)
		if !isGetOp {
			kv.setSession(op.ClientId, op.SequenceNum, reply)
		}
		return reply
	}
}

// CmdConfig
func (kv *ShardKV) applyConfig(latestConfig shardctrler.Config) CmdReply {
	if latestConfig.Num == kv.currentConfig.Num+1 {
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier updateLatestConfig to :%v", latestConfig)
		kv.updateShardStatus(latestConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = latestConfig
		return CmdReply{Status: OK}
	}
	DPrintf(kv.gid, kv.me, "ShardKVServerApplier rejectLatestConfig, due to configNum:%v != %v", latestConfig.Num, kv.currentConfig.Num+1)
	return CmdReply{Status: ErrOutdated}
}

func (kv *ShardKV) updateShardStatus(latestConfig shardctrler.Config) {
	for i := 0; i < shardctrler.NShards; i++ {
		// shard 当前不是我的group，但是，未来是我的group
		if kv.currentConfig.Shards[i] != kv.gid && latestConfig.Shards[i] == kv.gid {
			if gid := kv.currentConfig.Shards[i]; gid != 0 {
				kv.shardStore[i].Status = ShardPulling
			}
		}
		// shard 当前是我的group，但是，未来不是我的group
		if kv.currentConfig.Shards[i] == kv.gid && latestConfig.Shards[i] != kv.gid {
			if gid := latestConfig.Shards[i]; gid != 0 {
				kv.shardStore[i].Status = ShardBePulling
			}
		}
	}
}

// CmdInsertShards
func (kv *ShardKV) applyInsertShards(reply ShardsOpReply) CmdReply {
	if reply.ConfigNum == kv.currentConfig.Num {
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier applyInsertShards, before:%v, replyShards:%v", kv.shardStore, reply.Shards)
		for shardId, shardData := range reply.Shards {
			shard := kv.shardStore[shardId]
			// 自己的kv.monitorPull中发出command，标志 从其它gid的leader pull的reply返回了
			if shard.Status == ShardPulling {
				for key, value := range shardData {
					shard.KV[key] = value
				}
				// 标志gc：leader已经拉取到了，告诉对方leader可以gc了
				shard.Status = ShardNotifyPeerGidGC
			} else {
				DPrintf(kv.gid, kv.me, "ShardKVServerApplier applyInsertShards gotDuplicatedReply ignore")
				break
			}
		}
		for clientId, operation := range reply.LastOperations {
			if _, ok := kv.sessions[clientId]; !ok {
				kv.sessions[clientId] = operation
			}
		}
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier applyInsertShards, after:%v", kv.shardStore)
		return CmdReply{Status: OK}
	}
	DPrintf(kv.gid, kv.me, "ShardKVServerApplier rejectOutdatedInsertShards:%d", reply.ConfigNum)
	return CmdReply{Status: ErrOutdated}
}

// CmdDeleteShards
func (kv *ShardKV) applyDeleteShards(args ShardsOpArgs) CmdReply {
	if args.ConfigNum == kv.currentConfig.Num {
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier applyDeleteShards, before:%v, argsShards:%v", kv.shardStore, args.ShardIDs)
		for _, shardId := range args.ShardIDs {
			shard := kv.shardStore[shardId]
			if shard.Status == ShardNotifyPeerGidGC { // pull完成后，在monitorGC中发出command，告诉自己 已经通知对方gid的leader gc成功了，因此需要再设置回Serving
				shard.Status = ShardServing
			} else if shard.Status == ShardBePulling { // 对方gid的monitorGC中发出的 DeleteShardsData RPC 触发的command，即通知我已经pull完了，可以将它清空了
				kv.shardStore[shardId] = NewShard()
				shard.Status = ShardNotServing
			} else {
				DPrintf(kv.gid, kv.me, "ShardKVServerApplier applyDeleteShards gotDuplicatedReply ignore")
				break
			}
		}
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier applyDeleteShards, after:%v", kv.shardStore)
		return CmdReply{Status: OK}
	}
	DPrintf(kv.gid, kv.me, "ShardKVServerApplier rejectOutdatedDeleteShards:%d", args.ConfigNum)
	return CmdReply{Status: ErrOutdated}
}

// CmdNoop
func (kv *ShardKV) applyNoop() CmdReply {
	return CmdReply{Status: OK}
}
