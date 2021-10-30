package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"log"
	"os"
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

	gLog     *log.Logger
	gLogFile *os.File
}

func (kv *ShardKV) Command(args *CmdOpArgs, reply *CmdReply) {
	kv.mu.RLock()
	isGetOp := args.OpType == CmdOpGet
	if !isGetOp && kv.isDuplicated(args.ClientId, args.SequenceNum) {
		lastReply := kv.sessions[args.ClientId].Reply
		reply.Status = lastReply.Status
		reply.Response = lastReply.Response
		kv.DPrintf(kv.gid, kv.me, "ShardKVServer<-[%d:%d] replyDuplicatedResponse:%s", args.ClientId, args.SequenceNum, reply)
		kv.mu.RUnlock()
		return
	}
	if !kv.canServe(key2shard(args.Key)) {
		kv.DPrintf(kv.gid, kv.me, "ShardKVServer<-[%d:%d] canNotServeKey:%s", args.ClientId, args.SequenceNum, args.Key)
		reply.Status = ErrWrongGroup
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
	kv.DPrintf(kv.gid, kv.me, "ShardKVServer Leader startCommand <%d,%d> <%s:%s>", term, index, cmd.CmdType, cmd.CmdArgs)

	kv.mu.Lock()
	kv.buildNotifyCh(index)
	ch := kv.notifyCh[index]
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Status = res.Status
		reply.Response = res.Response
		kv.DPrintf(kv.gid, kv.me, "ShardKVServer reply <%s:%s> for %s", cmd.CmdType, reply, cmd.CmdArgs)
	case <-time.After(ExecuteTimeout):
		reply.Status = ErrTimeout
		reply.LeaderHint = kv.rf.GetLeader()
		kv.DPrintf(kv.gid, kv.me, "ShardKVServer timeout <%s>", cmd.CmdType)
	}

	go kv.releaseNotifyCh(index)
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd, testNum string) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CmdOpArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardsOpArgs{})
	labgob.Register(ShardsOpReply{})

	applyCh := make(chan raft.ApplyMsg)
	glog, glogFile := initGlog(testNum, gid)
	kv := &ShardKV{
		me:           me,
		rf:           raft.StartNode(servers, me, persister, applyCh, glog),
		rfPersister:  persister,
		applyCh:      applyCh,
		make_end:     make_end,
		gid:          gid,
		ctrlers:      ctrlers,
		maxraftstate: maxraftstate,
		dead:         0,
		sc:           shardctrler.MakeClerk(ctrlers),
		notifyCh:     make(map[int]chan CmdReply),
		gLog:         glog,
		gLogFile:     glogFile,
	}
	kv.installSnapshot(kv.rfPersister.ReadSnapshot())

	go kv.applier()
	go kv.monitorConfiguration()
	go kv.monitorPull()
	go kv.monitorGC()
	go kv.monitorNeedNoop()

	kv.DPrintf(kv.gid, kv.me, "ShardKVServer init success shardStore:%v", kv.shardStore)

	return kv
}
