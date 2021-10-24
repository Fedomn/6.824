package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"sync"
	"time"
)

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
	shardStore  ShardStore
	sessions    map[int64]LastOperation
	notifyCh    map[int]chan CmdOpReply
}

func (kv *ShardKV) Command(args *CmdOpArgs, reply *CmdOpReply) {
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

	index, term, isLeader := kv.rf.Start(Command{
		CmdType: CmdOp,
		CmdArgs: CmdOpArgs{
			OpType:      args.OpType,
			Key:         args.Key,
			Value:       args.Value,
			ClientId:    args.ClientId,
			SequenceNum: args.SequenceNum,
		},
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

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CmdOpArgs{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &ShardKV{
		me:           me,
		rf:           raft.StartNode(servers, me, persister, applyCh),
		rfPersister:  persister,
		applyCh:      applyCh,
		make_end:     make_end,
		gid:          gid,
		ctrlers:      ctrlers,
		maxraftstate: maxraftstate,
		dead:         0,
		sc:           shardctrler.MakeClerk(ctrlers),
		notifyCh:     make(map[int]chan CmdOpReply),
	}
	kv.installSnapshot(kv.rfPersister.ReadSnapshot())

	go kv.applier()

	DPrintf(kv.gid, kv.me, "ShardKVServer init success shardStore:%v", kv.shardStore)

	return kv
}
