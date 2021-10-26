package raft

import (
	"encoding/binary"
	"time"
)

// raft上层应用会调用它，来判断是否需要install snapshot，并设置raft state
// 这里也可以一致返回true，这样上层应用会重复install snapshot
// 正常能够调用这个方法由于，向applyCh里放入SnapshotValid类型的ApplyMsg，也就是follower被leader主动InstallSnapshot RPC后
func (rf *Raft) CondInstallSnapshot(snapshotTerm int, snapshotIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf(rf.me, "CondInstallSnapshot begin, snapshotIndex:%v commitIndex:%v lastApplied:%v",
		snapshotIndex, rf.commitIndex, rf.lastApplied)
	if snapshotIndex <= rf.commitIndex {
		// 一旦进入commitIndex，最终都会被apply，因此也不需要install snapshot了
		rf.DPrintf(rf.me, "CondInstallSnapshot outdated snapshotIndex:%v <= commitIndex:%v, no need to install snapshot", snapshotIndex, rf.commitIndex)
		return false
	}

	// rf.commitIndex < snapshotIndex
	if snapshotIndex > rf.getLastLogIndex() {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = rf.getEntriesToEnd(snapshotIndex)
	}
	rf.log[0].Command = nil
	rf.log[0].Term = snapshotTerm
	rf.log[0].Index = snapshotIndex
	rf.commitIndex = snapshotIndex
	rf.lastApplied = snapshotIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
	rf.DPrintf(rf.me, "CondInstallSnapshot installed snapshot success, log:%v, commitIndex:%v", rf.log, rf.commitIndex)
	return true
}

// 这里只传一个snapshot的原因：简化kv模型，由于test里每次cfg.one一个值，相当于对一个相同的key不停的set value
// 因此，我们只需要设置最后一次的value=snapshot就可以
// 能够调用这个方法是由于，向applyCh里放入CommandValid类型的ApplyMsg，达到一定size则执行，不论raft是什么state
func (rf *Raft) Snapshot(snapshotIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf(rf.me, "Snapshot begin, snapshotIndex:%v", snapshotIndex)

	// 如果snapshot index在commitIndex之后，说明还不能打snapshot，必须是committed的log才能认为可以snapshot
	if snapshotIndex > rf.commitIndex {
		rf.DPrintf(rf.me, "Snapshot snapshotIndex:%v > commitIndex:%v, so reject this snapshot operation",
			snapshotIndex, rf.commitIndex)
		return
	}

	// shrink logs
	rf.log = rf.getEntriesToEnd(snapshotIndex)
	rf.log[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
	rf.DPrintf(rf.me, "Snapshot snapshotIndex:%v snapshot:%v, after log:%v", snapshotIndex, binary.BigEndian.Uint32(snapshot), rf.log)
}

func (rf *Raft) asyncSnapshoter() {
	for {
		select {
		case msg := <-rf.asyncSnapshotCh:
			rf.applyCh <- msg
		case <-rf.killCh:
			return
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	now := time.Now()
	rf.sendWait(Event{Type: EventSnap, From: args.LeaderId, To: rf.me, Term: args.Term, Args: args, Reply: reply, DoneC: make(chan struct{})})
	duration := time.Now().Sub(now)
	rf.TRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v consume time:%s", rf.me, args.LeaderId, duration)
}
