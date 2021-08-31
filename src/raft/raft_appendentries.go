package raft

import (
	"context"
	"sync"
	"time"
)

// The AppendEntriesTicker goroutine to send appendEntries RPC when current status is leader
func (rf *Raft) AppendEntriesTicker() {
	var rpcMutex sync.Mutex
	var lastRpcCancel context.CancelFunc

	// 开始AppendEntries RPC(也是heartbeats) 循环
	tickerIsUp := make(chan struct{})
	tickerCtx, tickerCancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-time.After(heartbeatsTimeout * time.Millisecond):
				tickerIsUp <- struct{}{}
				continue
			case <-rf.resetAppendEntriesSignal:
				TPrintf(rf.me, "Raft: %+v will reset append entries timeout", rf.me)
				rpcMutex.Lock()
				if lastRpcCancel != nil {
					lastRpcCancel()
				}
				rpcMutex.Unlock()
				continue
			case <-tickerCtx.Done():
				return
			}
		}
	}()

	for rf.killed() == false {

		// 发送AppendEntries RPC 并收集reply
		select {
		case <-tickerIsUp:
			if !rf.isLeaderWithLock() {
				continue
			}
			rpcCtx, rpcCancel := context.WithCancel(context.Background())
			if lastRpcCancel != nil {
				lastRpcCancel()
			}
			rpcMutex.Lock()
			lastRpcCancel = rpcCancel // 第一次初始化 + 第n次赋值
			rpcMutex.Unlock()
			go rf.startAppendEntries(rpcCtx)
			continue
		}
	}

	tickerCancel()
	if lastRpcCancel != nil {
		lastRpcCancel()
	}
}

func (rf *Raft) startAppendEntries(ctx context.Context) {
	if rf.getStatusWithLock() != StateLeader {
		return
	}

	onceState := sync.Once{}

	rf.safe(func() {
		if rf.commitIndex == rf.getLastLogIndex() {
			// 没有uncommitted log entries，则append empty heartbeat
			TPrintf(rf.me, "AppendEntries no uncommitted log entries")
		} else {
			TPrintf(rf.me, "AppendEntries has uncommitted log entries")
		}
		rf.setNextIndexAndMatchIndex(rf.me, len(rf.getEntriesToEnd(rf.nextIndex[rf.me])))
	})

	for idx := range rf.peers {
		if idx == rf.me { // ignore itself
			continue
		}
		peerIdx := idx
		go func() {
			var args *AppendEntriesArgs
			rf.safe(func() {
				nextLogEntryIndex := rf.nextIndex[peerIdx]

				args = &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextLogEntryIndex - 1,
					PrevLogTerm:  rf.getLogEntry(nextLogEntryIndex - 1).Term,
					Entries:      rf.getEntriesToEnd(nextLogEntryIndex),
					LeaderCommit: rf.commitIndex,
				}
			})

			reply := &AppendEntriesReply{}

			// FIXME ?
			if !rf.isLeaderWithLock() {
				return
			}
			// RPC请求存在delay或hang住情况
			rpcDone := make(chan bool, 1)
			go func() {
				DPrintf(rf.me, "AppendEntries %v->%v send RPC %+v", rf.me, peerIdx, args)
				if ok := rf.sendAppendEntries(peerIdx, args, reply); !ok {
					TPrintf(rf.me, "AppendEntries %v->%v RPC not reply", rf.me, peerIdx)
					// 如果server not reply，则直接退出，等待下一轮RPC
					return
				}
				rpcDone <- true
			}()

			select {
			case <-ctx.Done():
				rf.safe(func() {
					// heartbeat time is up，忽略掉当前RPC的reply，直接进入下一轮
					TPrintf(rf.me, "AppendEntries %v->%v appendEntries timeout, start next appendEntries and mark unhealthy", rf.me, peerIdx)
					rf.peersHealthStatus[peerIdx] = false
				})
				return
			case <-rpcDone:
				rf.safe(func() {
					if isHealthy, ok := rf.peersHealthStatus[peerIdx]; ok && !isHealthy {
						TPrintf(rf.me, "RequestVote %v->%v RPC timeout recover and mark healthy", rf.me, peerIdx)
					}
					rf.peersHealthStatus[peerIdx] = true
				})
			}

			TPrintf(rf.me, "AppendEntries %v->%v RPC got %+v %+v", rf.me, peerIdx, reply, args)

			rf.safe(func() {
				rf.appendEntriesCnt++
			})

			if reply.Term > rf.getCurrentTermWithLock() && reply.Success == false {
				onceState.Do(func() {
					rf.safe(func() {
						DPrintf(rf.me, "AppendEntries %v->%v %s currentTerm %v got higher term %v, so revert to follower immediately",
							rf.me, peerIdx, rf.state, rf.currentTerm, reply.Term)
						rf.becomeFollower(reply.Term, None)
					})
				})
				return
			}

			if reply.Success {
				rf.safe(func() {
					rf.appendEntriesSuccessCnt++
					if len(args.Entries) == 0 {
						TPrintf(rf.me, "AppendEntries %v->%v RPC got success, entries len: %v, so heartbeat will do nothing",
							rf.me, peerIdx, len(args.Entries))
					} else {
						rf.setNextIndexAndMatchIndex(peerIdx, len(args.Entries))
						TPrintf(rf.me, "AppendEntries %v->%v RPC got success, entries len: %v, so will increase nextIndex %v, matchIndex %v",
							rf.me, peerIdx, len(args.Entries), rf.nextIndex, rf.matchIndex)
					}
				})
			} else {
				rf.safe(func() {
					rf.nextIndex[peerIdx] = reply.ConflictIndex
					DPrintf(rf.me, "AppendEntries %v->%v RPC got false, so will decrease nextIndex and append again, %v",
						rf.me, peerIdx, rf.nextIndex)
				})
				return
			}

			if rf.isGotMajorityAppendSuccessWithLock() {
				onceState.Do(func() {
					rf.safe(func() {
						rf.commitIndex = rf.calcCommitIndex()
						DPrintf(rf.me, "AppendEntries %v->%v got majority success, after calc commitIndex: %v, log: %v, matchIndex: %v",
							rf.me, peerIdx, rf.commitIndex, rf.log, rf.matchIndex)

						deltaLogsCount := rf.commitIndex - rf.lastApplied
						if deltaLogsCount > 0 {
							DPrintf(rf.me, "AppendEntries %v->%v will apply %v - %v = delta %v",
								rf.me, peerIdx, rf.commitIndex, rf.lastApplied, deltaLogsCount)
							go rf.applyLogsWithLock()
						}
					})
				})
				return
			}

			if rf.isEncounterPartitionWithLock() {
				DPrintf(rf.me, "AppendEntries %v->%v can't got majority success, will retry", rf.me, peerIdx)
				return
			}
		}()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer TPrintf(rf.me, "AppendEntries %v<-%v reply %+v %+v", rf.me, args.LeaderId, reply, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(rf.me, "AppendEntries %v<-%v current log %v, commitIndex: %v",
		rf.me, args.LeaderId, rf.log, rf.commitIndex)

	// 异常情况：leader term < follower term，说明leader已经在集群中落后了，返回false
	// 比如：一个leader刚从crash中recover，但它已经落后了很多term了，则它的logs也属于落后的
	if args.Term < rf.currentTerm {
		DPrintf(rf.me, "AppendEntries %v<-%v currentTerm %v > term %v, ignore lower term",
			rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 这里不像RequestVote必须要求args.Term > rf.currentTerm，因为AppendEntries会作为heartbeats保持
	// 所以rf.currentTerm需要保持和leader发来的args.Term一致
	if args.Term == rf.currentTerm {
		// Do nothing 正常情况
	}

	// 正常情况：leader term > follower term
	// args.Term > rf.currentTerm

	// 如果一个非follower状态的server，走到了这一步，说明集群中出现了 更新的server
	// 则它要立即 revert to follower
	// 变为leader后，继续做consistency check，像一个普通的follower一样
	if rf.state != StateFollower {
		DPrintf(rf.me, "AppendEntries %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately",
			rf.me, args.LeaderId, rf.state, rf.currentTerm, args.Term)
		rf.becomeFollower(args.Term, None)
	}

	// consistency check：prevLogIndex所在的log entry，它的term不等于prevLogTerm。
	passCheck := true
	for loop := true; loop; loop = false {
		// 原因：虽然follower认为已经committed的log，但整个集群并不认为，所以每次需要leader的overwrite
		// 一致性检测 和 leaderCommit 设置没有关系，一致性检测用来 check是否和leader是up-to-date的
		lastLogIndex := rf.getLastLogIndex()
		if args.PrevLogIndex > lastLogIndex {
			// 防止slice越界
			passCheck = false
			reply.ConflictIndex = lastLogIndex
			DPrintf(rf.me, "AppendEntries %v<-%v fail consistency for index. %v < %v, conflictIndex:%v",
				rf.me, args.LeaderId, lastLogIndex, args.PrevLogIndex, reply.ConflictIndex)
			break
		}

		// Optimization: when rejecting an AppendEntries request, the follower can include the term of
		// the conflicting entry and the first index it stores for that term.
		// With this information, the leader can decrement nextIndex to bypass all the conflicting entries in that term
		// one AppendEntries RPC will be required for each term with conflicting entries, rather than one RPC per entry

		matchedIndexLogTerm := rf.getLogEntry(args.PrevLogIndex).Term
		if matchedIndexLogTerm != args.PrevLogTerm {
			passCheck = false
			reply.ConflictIndex = rf.getFirstIndexOfTerm(matchedIndexLogTerm)
			DPrintf(rf.me, "AppendEntries %v<-%v fail consistency for term. %v != %v, conflictIndex:%v",
				rf.me, args.LeaderId, matchedIndexLogTerm, args.PrevLogTerm, reply.ConflictIndex)
			break
		}
	}

	if passCheck {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm

		TPrintf(rf.me, "AppendEntries %v<-%v pass consistency check", rf.me, args.LeaderId)
		reply.Success = true

		rf.log = rf.log[:args.PrevLogIndex+1]
		for i := range args.Entries {
			rf.log = append(rf.log, LogEntry{
				Command: args.Entries[i].Command,
				Term:    args.Entries[i].Term,
			})
		}

		// 如果leader commit index > follower 本地存储的 commit index，
		// 则更新 follower本地的 commitIndex = min(leaderCommit , 将要保存的logs中最后一个log entry index)
		if args.LeaderCommit > rf.commitIndex {
			// 存在一种情况，follower落后leader很多，这次appendEntries还未补全所有log，
			// 所以 这次follower的committedIndex为最后一个logIndex
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			go rf.applyLogsWithLock()
		}
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}

	rf.resetElectionSignal <- struct{}{}
	rf.resetAppendEntriesSignal <- struct{}{}
	return
}

func (rf *Raft) applyLogsWithLock() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.getLogEntry(i).Command,
			CommandIndex: i,
		}
		rf.lastApplied++
	}
}
