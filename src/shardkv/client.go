package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	leaderIds   map[int]int64 // gid -> leaderId
	clientId    int64
	sequenceNum int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:          shardctrler.MakeClerk(ctrlers),
		make_end:    make_end,
		leaderIds:   make(map[int]int64),
		clientId:    nrand(),
		sequenceNum: 0,
	}
	ck.config = ck.sm.Query(-1)
	CDPrintf(ck.clientId, "ShardKVClient initCfg:%v", ck.config)
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := &CmdOpArgs{
		OpType:      CmdOpGet,
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	return ck.Command(args)
}

func (ck *Clerk) Put(key string, value string) {
	args := &CmdOpArgs{
		OpType:      CmdOpPut,
		Key:         key,
		Value:       value,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.Command(args)
}
func (ck *Clerk) Append(key string, value string) {
	args := &CmdOpArgs{
		OpType:      CmdOpAppend,
		Key:         key,
		Value:       value,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.Command(args)
}

func (ck *Clerk) Command(args *CmdOpArgs) string {
	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, existGID := ck.config.Groups[gid]; existGID {
			leaderId := ck.leaderIds[gid]
			reply := &CmdReply{}
			ok := ck.make_end(servers[leaderId]).Call("ShardKV.Command", args.clone(), reply)
			switch {
			case ok && reply.Status == OK:
				ck.sequenceNum++
				return reply.Response
			case ok && reply.Status == ErrWrongGroup:
				goto refreshCfg
			default:
				if ok {
					CDPrintf(ck.clientId, "ShardKVClient gotErrReply:%s", reply.Status)
				}
				ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % int64(len(servers))
				continue
			}
		}
	refreshCfg:
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
		CDPrintf(ck.clientId, "ShardKVClient refreshCfg:%v", ck.config)
	}
}
