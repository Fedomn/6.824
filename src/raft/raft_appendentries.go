package raft

import "time"

func (rf *Raft) startAppendEntries(isHeartbeat bool) {
	rf.setNextIndexAndMatchIndex(rf.me, rf.getLastLogIndex()-rf.nextIndex[rf.me]+1)

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		peerIdx := idx
		// 移出goroutine，这里存在并发情况，可能currentTerm受到其它RPC而增加了，并非最开始的term
		nextLogEntryIndex := rf.nextIndex[peerIdx]
		prevLogIndex := nextLogEntryIndex - 1
		if prevLogIndex < rf.getFirstLogIndex() {
			// install snapshot
			args := &InstallSnapshotArgs{
				Seq:               rf.incSendRpcLatestSendSeq(peerIdx),
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.getFirstLogIndex(),
				LastIncludedTerm:  rf.getFirstLogTerm(),
				Data:              rf.persister.ReadSnapshot(),
			}
			reply := &InstallSnapshotReply{}
			go func() {
				now := time.Now()
				DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v sendRPC %+v", rf.me, peerIdx, args)
				if ok := rf.peers[peerIdx].Call("Raft.InstallSnapshot", args, reply); !ok {
					if !rf.killed() {
						TRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v RPC not reply", rf.me, peerIdx)
					}
				} else {
					duration := time.Now().Sub(now)
					rf.send(Event{Type: EventSnapResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
					TRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v consume time:%s", rf.me, peerIdx, duration)
				}
			}()
		} else {
			// append entries
			if !isHeartbeat {
				// inflight control
				rpcSeqStatus := rf.sendRpcLatestSeq[peerIdx]
				inflightCnt := rpcSeqStatus.SendSeq - rpcSeqStatus.RecvSeq
				if inflightCnt >= maxInflightAppCnt {
					DPrintf(rf.me, "InflightAppControl[%d] SendSeq:%v - RecvSeq:%v = inflightCnt:%v >= %d", peerIdx, rpcSeqStatus.SendSeq, rpcSeqStatus.RecvSeq, inflightCnt, maxInflightAppCnt)
					continue
				}
			}
			entries := make([]LogEntry, 0)
			if nextLogEntryIndex <= rf.getLastLogIndex() {
				entries = rf.getEntriesToEnd(nextLogEntryIndex)
			}
			args := &AppendEntriesArgs{
				Seq:          rf.incSendRpcLatestSendSeq(peerIdx),
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextLogEntryIndex - 1,
				PrevLogTerm:  rf.getLogEntry(nextLogEntryIndex - 1).Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			go func() {
				now := time.Now()
				DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v sendRPC %+v", rf.me, peerIdx, args)
				if ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply); !ok {
					if !rf.killed() {
						TRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v RPC not reply", rf.me, peerIdx)
					}
				} else {
					duration := time.Now().Sub(now)
					rf.send(Event{Type: EventAppResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
					TRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v consume time:%s", rf.me, peerIdx, duration)
				}
			}()
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now()
	rf.sendWait(Event{Type: EventApp, From: args.LeaderId, To: rf.me, Term: args.Term, Args: args, Reply: reply, DoneC: make(chan struct{})})
	duration := time.Now().Sub(now)
	TRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v consume time:%s", rf.me, args.LeaderId, duration)
}
