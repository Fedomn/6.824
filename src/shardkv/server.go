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
	notifyCh    map[int]chan CmdReply

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config
}

func (kv *ShardKV) Command(args *CmdOpArgs, reply *CmdReply) {
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

	kv.StartCmdAndWait(Command{CmdOp, *args}, reply)
}

func (kv *ShardKV) StartCmdAndWait(cmd Command, reply *CmdReply) {
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Status = ErrWrongLeader
		reply.LeaderHint = kv.rf.GetLeader()
		return
	}
	if cmd.CmdType == CmdOp {
		args := cmd.CmdArgs.(CmdOpArgs)
		DPrintf(kv.gid, kv.me, "ShardKVServer<-[%d:%d] Leader start%sCommand <%d,%d> args:%s", args.ClientId, args.SequenceNum, cmd.CmdType, term, index, args)
	} else {
		DPrintf(kv.gid, kv.me, "ShardKVServer Leader start%sCommand <%d,%d> args:%s", cmd.CmdType, term, index, cmd.CmdArgs)
	}

	kv.mu.Lock()
	kv.buildNotifyCh(index)
	ch := kv.notifyCh[index]
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Status = res.Status
		reply.Response = res.Response
		if cmd.CmdType == CmdOp {
			args := cmd.CmdArgs.(CmdOpArgs)
			DPrintf(kv.gid, kv.me, "ShardKVServer->[%d:%d] reply%s:%s", args.ClientId, args.SequenceNum, cmd.CmdType, reply)
		} else {
			DPrintf(kv.gid, kv.me, "ShardKVServer reply%s:%s", cmd.CmdType, reply)
		}
	case <-time.After(ExecuteTimeout):
		reply.Status = ErrTimeout
		reply.LeaderHint = kv.rf.GetLeader()
		if cmd.CmdType == CmdOp {
			args := cmd.CmdArgs.(CmdOpArgs)
			DPrintf(kv.gid, kv.me, "ShardKVServer->[%d:%d] timeout%s", args.ClientId, args.SequenceNum, cmd.CmdType)
		} else {
			DPrintf(kv.gid, kv.me, "ShardKVServer timeout%s", cmd.CmdType)
		}
	}

	go kv.releaseNotifyCh(index)
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CmdOpArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardsOpArgs{})
	labgob.Register(ShardsOpReply{})

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
		notifyCh:     make(map[int]chan CmdReply),
	}
	kv.installSnapshot(kv.rfPersister.ReadSnapshot())

	go kv.applier()
	go kv.monitorConfiguration()
	go kv.monitorPull()
	go kv.monitorGC()

	DPrintf(kv.gid, kv.me, "ShardKVServer init success shardStore:%v", kv.shardStore)

	return kv
}
