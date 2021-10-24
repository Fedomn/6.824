package shardkv

import "fmt"

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
				case CmdInsertShards:
				case CmdDeleteShards:
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

func (kv *ShardKV) applyOp(op CmdOpArgs) CmdReply {
	if kv.isOutdatedCommand(op.ClientId, op.SequenceNum) {
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier gotOutdatedCommand:[%d,%d]", op.ClientId, op.SequenceNum)
		kv.mu.RUnlock()
		return CmdReply{Status: ErrOutdated}
	}

	if isDuplicated, lastReply := kv.getDuplicatedCommandReply(op.ClientId, op.SequenceNum); isDuplicated {
		DPrintf(kv.gid, kv.me, "ShardKVServerApplier gotDuplicatedCommand:[%d,%d]", op.ClientId, op.SequenceNum)
		return lastReply
	} else {
		shardId := key2shard(op.Key)
		reply := kv.applyToStore(op, shardId)
		if op.OpType == CmdOpPut {
			DPrintf(kv.gid, kv.me, "ShardKVServerApplier CmdOpPut:%s, KVStore:%v", op, kv.shardStore)
		}
		kv.setSession(op.ClientId, op.SequenceNum, reply)
		return reply
	}
}
