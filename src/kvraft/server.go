package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"sync"
	"sync/atomic"
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

type LastOperation struct {
	SequenceNum int64
	Reply       CommandReply
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	lastApplied int
	kvStore     map[string]string
	sessions    map[int64]LastOperation
	notifyCh    map[int]chan CommandReply
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.RLock()
	if kv.isOutdatedCommand(args.ClientId, args.SequenceNum) {
		DPrintf(kv.me, "KVServer reply outdated command SequenceNum:%v", args.SequenceNum)
		reply.Status = ErrOutdated
		kv.mu.RUnlock()
		return
	}
	if isDuplicated, lastReply := kv.getDuplicatedCommandReply(args.ClientId, args.SequenceNum); isDuplicated {
		DPrintf(kv.me, "KVServer reply duplicated response")
		reply.Status = lastReply.Status
		reply.Response = lastReply.Response
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(Op{
		OpType:      args.OpType,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	})
	if !isLeader {
		//DPrintf(kv.me, "KVServer reply ErrWrongLeader")
		reply.Status = ErrWrongLeader
		reply.LeaderHint = kv.rf.GetLeader()
		return
	}

	DPrintf(kv.me, "KVServer Leader startCommand args:%+v", args)

	kv.mu.Lock()
	kv.buildNotifyCh(index)
	ch := kv.notifyCh[index]
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Status = res.Status
		reply.Response = res.Response
	case <-time.After(ExecuteTimeout):
		reply.Status = ErrTimeout
		reply.LeaderHint = kv.rf.GetLeader()
	}

	//DPrintf(kv.me, "KVServer Leader gotReply:%v", reply)

	go kv.releaseNotifyCh(index)
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			//DPrintf(kv.me, "KVServer gotApplyMsg:%+v", msg)
			switch {
			case msg.CommandValid:
				if msg.CommandIndex <= kv.lastApplied {
					DPrintf(kv.me, "KVServer discard outdated message")
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				op := msg.Command.(Op)
				reply := CommandReply{}

				if isDuplicated, lastReply := kv.getDuplicatedCommandReply(op.ClientId, op.SequenceNum); isDuplicated {
					reply = lastReply
				} else {
					reply = kv.applyToStore(op)
					kv.setSession(op.ClientId, op.SequenceNum, reply)
				}

				if _, isLeader := kv.rf.GetState(); isLeader {
					kv.notifyCh[msg.CommandIndex] <- reply
				}
			case msg.SnapshotValid:
			default:
				panic(fmt.Sprintf("Unexpected message %v", msg))
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) isOutdatedCommand(clientId, sequenceNum int64) bool {
	lastOperation, ok := kv.sessions[clientId]
	if ok {
		return lastOperation.SequenceNum > sequenceNum
	} else {
		return false
	}
}

func (kv *KVServer) getDuplicatedCommandReply(clientId, sequenceNum int64) (bool, CommandReply) {
	lastOperation, ok := kv.sessions[clientId]
	if ok {
		return lastOperation.SequenceNum == sequenceNum, lastOperation.Reply
	} else {
		return false, CommandReply{}
	}
}

// KVServer可能未收到client的RPC，而是由raft同步过来的command
func (kv *KVServer) setSession(clientId, sequenceNum int64, reply CommandReply) {
	//DPrintf(kv.me, "KVServer setSession for client:%v sequenceNum:%v", clientId, sequenceNum)
	kv.sessions[clientId] = LastOperation{sequenceNum, reply}
}

// 因为这里OpGet也会进入raft log，所以所有的op都会递增log index
func (kv *KVServer) buildNotifyCh(index int) {
	kv.notifyCh[index] = make(chan CommandReply)
}

func (kv *KVServer) releaseNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notifyCh, index)
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyToStore(op Op) CommandReply {
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
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.StartNode(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.lastApplied = 0
	kv.kvStore = make(map[string]string)
	kv.sessions = make(map[int64]LastOperation)
	kv.notifyCh = make(map[int]chan CommandReply)

	go kv.applier()

	return kv
}
