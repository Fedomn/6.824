package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"time"
)

const ExecuteTimeout = time.Second

type Op struct {
	OpType      OpType
	Args        CommandArgs
	ClientId    int64
	SequenceNum int64
}

func (o Op) String() string {
	return fmt.Sprintf("[%d:%d] %s<%s>", o.ClientId, o.SequenceNum, o.OpType, o.Args)
}

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	rfPersister *raft.Persister

	configStore *MemoryConfigStore

	lastApplied int
	kvStore     map[string]string
	sessions    map[int64]LastOperation
	notifyCh    map[int]chan CommandReply

	gLog *log.Logger
}

type LastOperation struct {
	SequenceNum int64
	Reply       CommandReply
}

func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	sc.mu.RLock()
	if isDuplicated, lastReply := sc.getDuplicatedCommandReply(args.ClientId, args.SequenceNum); isDuplicated {
		reply.Status = lastReply.Status
		reply.Config = lastReply.Config
		sc.DPrintf(sc.me, "ShardCtrlerServer<-[%d:%d] duplicatedResponse:%s", args.ClientId, args.SequenceNum, reply)
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	index, term, isLeader := sc.rf.Start(Op{
		OpType:      args.OpType,
		Args:        *args.clone(),
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	})
	if !isLeader {
		//sc.DPrintf(sc.me, "ShardCtrlerServer reply ErrWrongLeader")
		reply.Status = ErrWrongLeader
		return
	}

	sc.DPrintf(sc.me, "ShardCtrlerServer<-[%d:%d] Leader startCommand <%d,%d> args:%s", args.ClientId, args.SequenceNum, term, index, args)

	sc.mu.Lock()
	sc.buildNotifyCh(index)
	ch := sc.notifyCh[index]
	sc.mu.Unlock()

	select {
	case res := <-ch:
		reply.Status = res.Status
		reply.Config = res.Config
		sc.DPrintf(sc.me, "ShardCtrlerServer->[%d:%d] reply:%s", args.ClientId, args.SequenceNum, reply)
	case <-time.After(ExecuteTimeout):
		reply.Status = ErrTimeout
		sc.DPrintf(sc.me, "ShardCtrlerServer->[%d:%d] timeout", args.ClientId, args.SequenceNum)
	}

	go sc.releaseNotifyCh(index)
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			sc.mu.Lock()
			sc.DPrintf(sc.me, "ShardCtrlerServerApplier gotApplyMsg:%v", msg)
			switch {
			case msg.CommandValid:
				if msg.CommandIndex <= sc.lastApplied {
					sc.DPrintf(sc.me, "ShardCtrlerServerApplier discardOutdatedMsgIndex:%d lastApplied:%d", msg.CommandIndex, sc.lastApplied)
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = msg.CommandIndex

				op := msg.Command.(Op)
				reply := CommandReply{}

				if isDuplicated, lastReply := sc.getDuplicatedCommandReply(op.ClientId, op.SequenceNum); isDuplicated {
					sc.DPrintf(sc.me, "ShardCtrlerServerApplier gotDuplicatedCommand:[%d,%d]", op.ClientId, op.SequenceNum)
					reply = lastReply
				} else {
					reply = sc.applyToStore(op)
					sc.setSession(op.ClientId, op.SequenceNum, reply)
				}

				if currentTerm, isLeader := sc.rf.GetState(); isLeader {
					if msg.CommandTerm <= currentTerm {
						if ch, ok := sc.notifyCh[msg.CommandIndex]; ok {
							ch <- reply
						} else {
							sc.DPrintf(sc.me, "ShardCtrlerServerApplier gotApplyMsgTimeout index:%d", msg.CommandIndex)
						}
					} else {
						sc.DPrintf(sc.me, "ShardCtrlerServerApplier lostLeadership")
					}
				}
			default:
				panic(fmt.Sprintf("ShardCtrlerServerApplier Unexpected message %v", msg))
			}
			sc.mu.Unlock()
		case <-sc.rf.KillCh():
			return
		}
	}
}

func (sc *ShardCtrler) applyToStore(op Op) CommandReply {
	switch op.OpType {
	case OpJoin:
		sc.configStore.Join(op.Args.Servers)
		return CommandReply{Status: OK}
	case OpLeave:
		sc.configStore.Leave(op.Args.GIDs)
		return CommandReply{Status: OK}
	case OpMove:
		sc.configStore.Move(op.Args.Shard, op.Args.GID)
		return CommandReply{Status: OK}
	case OpQuery:
		config, status := sc.configStore.Query(op.Args.Num)
		return CommandReply{status, config}
	default:
		panic(fmt.Sprintf("invalid OpType: %v", op))
	}
}

func DefaultConfig() Config {
	return Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, testNum string) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.gLog = initGlog(testNum)
	sc.me = me
	labgob.Register(Op{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.StartNode(servers, me, persister, sc.applyCh, sc.gLog)
	sc.rfPersister = persister

	sc.lastApplied = 0
	sc.configStore = NewMemoryConfigStore()
	sc.sessions = make(map[int64]LastOperation)
	sc.notifyCh = make(map[int]chan CommandReply)

	go sc.applier()

	sc.DPrintf(sc.me, "ShardCtrlerServer init success")

	return sc
}
