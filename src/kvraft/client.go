package kvraft

import (
	"6.824/labrpc"
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

func (ck *Clerk) Get(key string) string {
	args := &CommandArgs{
		OpType:      OpGet,
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	return ck.Command(args)
}

func (ck *Clerk) Put(key string, value string) {
	args := &CommandArgs{
		OpType:      OpPut,
		Key:         key,
		Value:       value,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.Command(args)
}

func (ck *Clerk) Append(key string, value string) {
	args := &CommandArgs{
		OpType:      OpAppend,
		Key:         key,
		Value:       value,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.Command(args)
}

// 注意这里的ck.servers里的顺序不能保证，因为存在config.random_handles会打乱顺序
// 从而导致leaderHint并不是ck.servers里的index
func (ck *Clerk) Command(args *CommandArgs) string {
	for {
		// CDPrintf(ck.clientId, "KVClient startArgs:%s", args)
		reply := &CommandReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Command", args.clone(), reply)
		if !ok || reply.Status == ErrWrongLeader || reply.Status == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			if ok {
				CDPrintf(ck.clientId, "KVClient gotErrReply:%s", reply.Status)
			}
			continue
		}
		ck.sequenceNum++
		return reply.Response
	}
}
