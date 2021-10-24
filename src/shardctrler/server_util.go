package shardctrler

import (
	"6.824/raft"
	"sync/atomic"
)

func (sc *ShardCtrler) isOutdatedCommand(clientId, sequenceNum int64) bool {
	lastOperation, ok := sc.sessions[clientId]
	if ok {
		return lastOperation.SequenceNum > sequenceNum
	} else {
		return false
	}
}

func (sc *ShardCtrler) getDuplicatedCommandReply(clientId, sequenceNum int64) (bool, CommandReply) {
	lastOperation, ok := sc.sessions[clientId]
	if ok {
		return lastOperation.SequenceNum == sequenceNum, lastOperation.Reply
	} else {
		return false, CommandReply{}
	}
}

// KVServer可能未收到client的RPC，而是由raft同步过来的command
func (sc *ShardCtrler) setSession(clientId, sequenceNum int64, reply CommandReply) {
	//DPrintf(kv.me, "KVServer setSession for client:%v sequenceNum:%v", clientId, sequenceNum)
	sc.sessions[clientId] = LastOperation{sequenceNum, reply}
}

// 因为这里OpGet也会进入raft log，所以所有的op都会递增log index
func (sc *ShardCtrler) buildNotifyCh(index int) {
	sc.notifyCh[index] = make(chan CommandReply)
}

func (sc *ShardCtrler) releaseNotifyCh(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.notifyCh, index)
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}
