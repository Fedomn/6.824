package shardctrler

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers     []*labrpc.ClientEnd
	leaderId    int64
	clientId    int64
	sequenceNum int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:     servers,
		leaderId:    0,
		clientId:    nrand(),
		sequenceNum: 0,
	}
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{
		OpType:      OpQuery,
		Num:         num,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	return ck.command(args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{
		OpType:      OpJoin,
		Servers:     servers,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.command(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{
		OpType:      OpLeave,
		GIDs:        gids,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.command(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{
		OpType:      OpMove,
		Shard:       shard,
		GID:         gid,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.command(args)
}

func (ck *Clerk) command(args *CommandArgs) Config {
	for {
		reply := &CommandReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Command", args.clone(), reply)
		if !ok || reply.Status != OK {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			if ok {
				CDPrintf(ck.clientId, "KVClient gotErrReply:%s", reply.Status)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.sequenceNum++
		return reply.Config
	}
}
