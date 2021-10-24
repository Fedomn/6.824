package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"fmt"
	"sync"
	"time"
)

const ExecuteTimeout = time.Second

type Op struct {
	OpType      OpType
	Key         string
	Value       string
	ClientId    int64
	SequenceNum int64
}

func (o Op) String() string {
	return fmt.Sprintf("[%d:%d] %s<%s,%s>", o.ClientId, o.SequenceNum, o.OpType, o.Key, o.Value)
}

type LastOperation struct {
	SequenceNum int64
	Reply       CommandReply
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	rfPersister  *raft.Persister
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead         int32
	sc           *shardctrler.Clerk

	lastApplied int
	kvStore     map[string]string
	sessions    map[int64]LastOperation
	notifyCh    map[int]chan CommandReply
}

func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.RLock()
	if kv.isOutdatedCommand(args.ClientId, args.SequenceNum) {
		DPrintf(kv.gid, kv.me, "ShardKVServer<-[%d:%d] outdatedCommand", args.ClientId, args.SequenceNum)
		reply.Status = ErrOutdated
		kv.mu.RUnlock()
		return
	}
	if isDuplicated, lastReply := kv.getDuplicatedCommandReply(args.ClientId, args.SequenceNum); isDuplicated {
		reply.Status = lastReply.Status
		reply.Response = lastReply.Response
		DPrintf(kv.gid, kv.me, "ShardKVServer<-[%d:%d] duplicatedResponse:%s", args.ClientId, args.SequenceNum, reply)
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, term, isLeader := kv.rf.Start(Op{
		OpType:      args.OpType,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	})
	if !isLeader {
		reply.Status = ErrWrongLeader
		reply.LeaderHint = kv.rf.GetLeader()
		return
	}

	DPrintf(kv.gid, kv.me, "ShardKVServer<-[%d:%d] Leader startCommand <%d,%d> args:%s", args.ClientId, args.SequenceNum, term, index, args)

	kv.mu.Lock()
	kv.buildNotifyCh(index)
	ch := kv.notifyCh[index]
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Status = res.Status
		reply.Response = res.Response
		DPrintf(kv.gid, kv.me, "ShardKVServer->[%d:%d] reply:%s", args.ClientId, args.SequenceNum, reply)
	case <-time.After(ExecuteTimeout):
		reply.Status = ErrTimeout
		reply.LeaderHint = kv.rf.GetLeader()
		DPrintf(kv.gid, kv.me, "ShardKVServer->[%d:%d] timeout", args.ClientId, args.SequenceNum)
	}

	go kv.releaseNotifyCh(index)
}

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

				op := msg.Command.(Op)
				reply := CommandReply{}

				if kv.isOutdatedCommand(op.ClientId, op.SequenceNum) {
					DPrintf(kv.gid, kv.me, "ShardKVServerApplier gotOutdatedCommand:[%d,%d]", op.ClientId, op.SequenceNum)
					reply.Status = ErrOutdated
					kv.mu.RUnlock()
					return
				}

				if isDuplicated, lastReply := kv.getDuplicatedCommandReply(op.ClientId, op.SequenceNum); isDuplicated {
					DPrintf(kv.gid, kv.me, "ShardKVServerApplier gotDuplicatedCommand:[%d,%d]", op.ClientId, op.SequenceNum)
					reply = lastReply
				} else {
					reply = kv.applyToStore(op)
					if op.OpType == OpPut {
						DPrintf(kv.gid, kv.me, "ShardKVServerApplier OpPut:%s, KVStore:%v", op, kv.kvStore)
					}
					kv.setSession(op.ClientId, op.SequenceNum, reply)
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
					DPrintf(kv.gid, kv.me, "ShardKVServerApplier %d > %d willSnapshot kvStore:%v", beforeSize, kv.maxraftstate, kv.kvStore)
					kv.rf.Snapshot(msg.CommandIndex, kv.makeSnapshot())
					DPrintf(kv.gid, kv.me, "ShardKVServerApplier afterSnapshotKvSize:%d, %v", kv.rfPersister.RaftStateSize(), len(kv.kvStore))
				}
			case msg.SnapshotValid:
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.installSnapshot(msg.Snapshot)
					DPrintf(kv.gid, kv.me, "ShardKVServerApplier willInstallSnapshot:%v", kv.kvStore)
					kv.lastApplied = msg.SnapshotIndex
				}
			default:
				panic(fmt.Sprintf("ShardKVServerApplier Unexpected message %v", msg))
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) applyToStore(op Op) CommandReply {
	switch op.OpType {
	case OpGet:
		return CommandReply{Response: kv.kvStore[op.Key], Status: OK}
	case OpPut:
		kv.kvStore[op.Key] = op.Value
		return CommandReply{Response: kv.kvStore[op.Key], Status: OK}
	case OpAppend:
		kv.kvStore[op.Key] += op.Value
		return CommandReply{Response: kv.kvStore[op.Key], Status: OK}
	default:
		panic(fmt.Sprintf("invalid op: %v", op))
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.sc = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.StartNode(servers, me, persister, kv.applyCh)
	kv.rfPersister = persister

	kv.installSnapshot(kv.rfPersister.ReadSnapshot())
	kv.notifyCh = make(map[int]chan CommandReply)

	go kv.applier()

	DPrintf(kv.gid, kv.me, "ShardKVServer init success kvStoreSize:%v", kv.kvStore)

	return kv
}
