package shardkv

import (
	"6.824/raft"
	"sync/atomic"
)

func (kv *ShardKV) isOutdatedCommand(clientId, sequenceNum int64) bool {
	lastOperation, ok := kv.sessions[clientId]
	if ok {
		return lastOperation.SequenceNum > sequenceNum
	} else {
		return false
	}
}

func (kv *ShardKV) getDuplicatedCommandReply(clientId, sequenceNum int64) (bool, CmdReply) {
	lastOperation, ok := kv.sessions[clientId]
	if ok {
		return lastOperation.SequenceNum == sequenceNum, lastOperation.Reply
	} else {
		return false, CmdReply{}
	}
}

// KVServer可能未收到client的RPC，而是由raft同步过来的command
func (kv *ShardKV) setSession(clientId, sequenceNum int64, reply CmdReply) {
	//DPrintf(kv.me, "KVServer setSession for client:%v sequenceNum:%v", clientId, sequenceNum)
	kv.sessions[clientId] = LastOperation{sequenceNum, reply}
}

// 因为这里OpGet也会进入raft log，所以所有的op都会递增log index
func (kv *ShardKV) buildNotifyCh(index int) {
	kv.notifyCh[index] = make(chan CmdReply)
}

func (kv *ShardKV) releaseNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notifyCh, index)
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// needed by shardkv tester
func (kv *ShardKV) Raft() *raft.Raft {
	return kv.rf
}
