package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
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

func (o Op) String() string {
	return fmt.Sprintf("[%d:%d] %s<%s,%s>", o.ClientId, o.SequenceNum, o.OpType, o.Key, o.Value)
}

type LastOperation struct {
	SequenceNum int64
	Reply       CommandReply
}

type KVServer struct {
	mu          sync.RWMutex
	me          int
	rf          *raft.Raft
	rfPersister *raft.Persister
	applyCh     chan raft.ApplyMsg
	dead        int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	lastApplied int
	kvStore     map[string]string
	sessions    map[int64]LastOperation
	notifyCh    map[int]chan CommandReply
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.RLock()
	if kv.isOutdatedCommand(args.ClientId, args.SequenceNum) {
		DPrintf(kv.me, "KVServer<-[%d:%d] outdatedCommand", args.ClientId, args.SequenceNum)
		reply.Status = ErrOutdated
		kv.mu.RUnlock()
		return
	}
	if isDuplicated, lastReply := kv.getDuplicatedCommandReply(args.ClientId, args.SequenceNum); isDuplicated {
		reply.Status = lastReply.Status
		reply.Response = lastReply.Response
		DPrintf(kv.me, "KVServer<-[%d:%d] duplicatedResponse:%s", args.ClientId, args.SequenceNum, reply)
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

	DPrintf(kv.me, "KVServer<-[%d:%d] Leader startCommand args:%s", args.ClientId, args.SequenceNum, args)

	kv.mu.Lock()
	kv.buildNotifyCh(index)
	ch := kv.notifyCh[index]
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Status = res.Status
		reply.Response = res.Response
		DPrintf(kv.me, "KVServer->[%d:%d] reply:%s", args.ClientId, args.SequenceNum, reply)
	case <-time.After(ExecuteTimeout):
		reply.Status = ErrTimeout
		reply.LeaderHint = kv.rf.GetLeader()
		DPrintf(kv.me, "KVServer->[%d:%d] timeout", args.ClientId, args.SequenceNum)
	}

	go kv.releaseNotifyCh(index)
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			DPrintf(kv.me, "KVServerApplier gotApplyMsg:%v", msg)
			switch {
			case msg.CommandValid:
				if msg.CommandIndex <= kv.lastApplied {
					DPrintf(kv.me, "KVServerApplier discardOutdatedMsgIndex:%d lastApplied:%d", msg.CommandIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				op := msg.Command.(Op)
				reply := CommandReply{}

				if kv.isOutdatedCommand(op.ClientId, op.SequenceNum) {
					DPrintf(kv.me, "KVServerApplier gotOutdatedCommand:[%d,%d]", op.ClientId, op.SequenceNum)
					reply.Status = ErrOutdated
					kv.mu.RUnlock()
					return
				}

				if isDuplicated, lastReply := kv.getDuplicatedCommandReply(op.ClientId, op.SequenceNum); isDuplicated {
					DPrintf(kv.me, "KVServerApplier gotDuplicatedCommand:[%d,%d]", op.ClientId, op.SequenceNum)
					reply = lastReply
				} else {
					reply = kv.applyToStore(op)
					if op.OpType == OpPut {
						DPrintf(kv.me, "KVServerApplier OpPut:%s, KVStore:%v", op, kv.kvStore)
					}
					kv.setSession(op.ClientId, op.SequenceNum, reply)
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader {
					if msg.CommandTerm <= currentTerm {
						if ch, ok := kv.notifyCh[msg.CommandIndex]; ok {
							ch <- reply
						} else {
							DPrintf(kv.me, "KVServerApplier gotApplyMsgTimeout index:%d", msg.CommandIndex)
						}
					} else {
						DPrintf(kv.me, "KVServerApplier lostLeadership")
					}
				}

				if kv.maxraftstate != -1 && kv.rfPersister.RaftStateSize() > kv.maxraftstate {
					beforeSize := kv.rfPersister.RaftStateSize()
					DPrintf(kv.me, "KVServerApplier %d > %d willSnapshot kvStore:%v", beforeSize, kv.maxraftstate, kv.kvStore)
					kv.rf.Snapshot(msg.CommandIndex, kv.makeSnapshot())
					DPrintf(kv.me, "KVServerApplier afterSnapshotKvSize:%d, %v", kv.rfPersister.RaftStateSize(), len(kv.kvStore))
				}
			case msg.SnapshotValid:
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.installSnapshot(msg.Snapshot)
					DPrintf(kv.me, "KVServerApplier willInstallSnapshot:%v", kv.kvStore)
					kv.lastApplied = msg.SnapshotIndex
				}
			default:
				panic(fmt.Sprintf("KVServerApplier Unexpected message %v", msg))
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.lastApplied)
	_ = e.Encode(kv.kvStore)
	_ = e.Encode(kv.sessions)
	return w.Bytes()
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		kv.lastApplied = 0
		kv.kvStore = make(map[string]string)
		kv.sessions = make(map[int64]LastOperation)
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var kvStore map[string]string
	var sessions map[int64]LastOperation
	if d.Decode(&lastApplied) != nil || d.Decode(&kvStore) != nil || d.Decode(&sessions) != nil {
		log.Fatalf("KVServerApplier decode:%v error", snapshot)
	}
	kv.lastApplied = lastApplied
	kv.kvStore = kvStore
	kv.sessions = sessions
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
	kv.rf = raft.StartNode(servers, me, persister, kv.applyCh, nil)
	kv.rfPersister = persister

	// You may need initialization code here.
	kv.installSnapshot(kv.rfPersister.ReadSnapshot())
	kv.notifyCh = make(map[int]chan CommandReply)

	go kv.applier()

	DPrintf(kv.me, "KVServer init success kvStoreSize:%v", kv.kvStore)

	return kv
}
