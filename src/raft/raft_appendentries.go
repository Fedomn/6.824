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
				DPrintf(rf.me, "Raft: %+v will reset append entries timeout", rf.me)
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
	if rf.getStatusWithLock() != leader {
		return
	}

	// reset 计数器
	rf.safe(func() {
		// include leader itself
		rf.appendEntriesCnt = 1
		rf.appendEntriesSuccessCnt = 1
	})

	commitOnce := sync.Once{}

	rf.safe(func() {
		if rf.majorityCommittedIndex <= rf.getLastLogIndex() {
			// 没有uncommitted log entries，则append empty heartbeat
			rf.log = append(rf.log, LogEntry{
				Command: nil,
				Term:    rf.currentTerm,
			})
			DPrintf(rf.me, "AppendEntries no uncommitted log entries, so will append empty as heartbeat")
		}
	})

	for idx := range rf.peers {
		if idx == rf.me { // ignore itself
			continue
		}
		peerIdx := idx
		go func() {
			var args *AppendEntriesArgs
			rf.safe(func() {
				// 取min的目的：防止有的server已经append up-to-date后，由于没有到majority仍会retry
				// 所以，防止slice溢出，仍然取leader初始化时的nextIndex
				nextLogEntryIndex := min(rf.nextIndex[peerIdx], rf.committedIndex+1)
				// 这一轮leader希望follower commit的index为上一轮的majority reply success的index
				leaderCommitIndex := rf.majorityCommittedIndex

				// Tips leader在appendEntries会根据nextIndex，给follower补un-appended log entries
				// heartbeat只是entries为空的AppendEntries RPC
				entries := make([]LogEntry, 0)

				tmp := rf.getEntriesToEnd(nextLogEntryIndex)
				entries = append(entries, tmp...)

				prevLogIndex := nextLogEntryIndex - 1
				prevLogTerm := rf.log[prevLogIndex].Term

				args = &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommitIndex,
				}
			})

			reply := &AppendEntriesReply{}

			if !rf.isLeaderWithLock() {
				return
			}
			// RPC请求存在delay或hang住情况
			rpcDone := make(chan bool, 1)
			go func() {
				DPrintf(rf.me, "AppendEntries %v->%v send RPC %+v", rf.me, peerIdx, args)
				if ok := rf.sendAppendEntries(peerIdx, args, reply); !ok {
					DPrintf(rf.me, "AppendEntries %v->%v RPC not reply", rf.me, peerIdx)
					// 如果server not reply，则直接退出，等待下一轮RPC
					return
				}
				rpcDone <- true
			}()

			select {
			case <-ctx.Done():
				rf.safe(func() {
					// heartbeat time is up，忽略掉当前RPC的reply，直接进入下一轮
					DPrintf(rf.me, "AppendEntries %v->%v appendEntries timeout, start next appendEntries and mark unhealthy", rf.me, peerIdx)
					rf.peersHealthStatus[peerIdx] = false
				})
				return
			case <-rpcDone:
				rf.safe(func() {
					if isHealthy, ok := rf.peersHealthStatus[peerIdx]; ok && !isHealthy {
						DPrintf(rf.me, "RequestVote %v->%v RPC timeout recover and mark healthy", rf.me, peerIdx)
					}
					rf.peersHealthStatus[peerIdx] = true
				})
			}

			DPrintf(rf.me, "AppendEntries %v->%v RPC got %+v %+v", rf.me, peerIdx, reply, args)

			appendEntriesCnt := 0
			rf.safe(func() {
				rf.appendEntriesCnt++
				appendEntriesCnt = rf.appendEntriesCnt
			})

			if reply.Term > rf.getCurrentTermWithLock() && reply.Success == false {
				if rf.isFollowerWithLock() { // 如果已经在其它goroutine变成了follower，这里就不在处理
					return
				}
				DPrintf(rf.me, "AppendEntries %v->%v %s currentTerm %v got higher term %v, so revert to follower immediately",
					rf.me, peerIdx, rf.getStatusWithLock(), rf.getCurrentTermWithLock(), reply.Term)
				rf.safe(func() {
					rf.currentTerm = reply.Term
					rf.status = follower
					DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v, logs %v", rf.me, rf.status, rf.currentTerm, rf.log)

					rf.resetElectionSignal <- struct{}{}
					rf.resetAppendEntriesSignal <- struct{}{}
				})
				return
			}

			appendEntriesSuccessCnt := 0
			if reply.Success {
				rf.safe(func() {
					rf.appendEntriesSuccessCnt++
					appendEntriesSuccessCnt = rf.appendEntriesSuccessCnt
					// Tips 防止多次append成功的server日志，导致nextIndex溢出
					rf.nextIndex[peerIdx] = min(rf.nextIndex[peerIdx]+len(args.Entries), rf.committedIndex+1)
					rf.matchIndex[peerIdx] = rf.nextIndex[peerIdx]
					DPrintf(rf.me, "AppendEntries %v->%v RPC got success, entries len: %v, so will increase nextIndex %v",
						rf.me, peerIdx, len(args.Entries), rf.nextIndex)
				})
			} else {
				rf.safe(func() {
					rf.nextIndex[peerIdx]--
					DPrintf(rf.me, "AppendEntries %v->%v RPC got false, so will decrease nextIndex and append again, %v",
						rf.me, peerIdx, rf.nextIndex)
				})
				return
			}

			majorityCount := len(rf.peers)/2 + 1
			if appendEntriesSuccessCnt >= majorityCount {
				// Tips 不能通过复制到majority server来判断commit，通过下一个log commit来确保上一个确实commit. Log Matching Property.
				commitOnce.Do(func() {
					rf.safe(func() {
						rf.majorityCommittedIndex++
						rf.committedIndex = rf.majorityCommittedIndex - 1
						DPrintf(rf.me, "AppendEntries %v->%v got majority success, so commit these log entries", rf.me, peerIdx)
						DPrintf(rf.me, "AppendEntries %v->%v after commit, committedIndex: %v, log: %v", rf.me, peerIdx, rf.committedIndex, rf.log)
					})
				})
				return
			}

			if appendEntriesCnt >= majorityCount {
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
	defer DPrintf(rf.me, "AppendEntries %v<-%v reply %+v %+v", rf.me, args.LeaderId, reply, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 异常情况：follower term > leader term，说明leader已经在集群中落后了，返回false
	// 比如：一个follower刚从crash中recover，但它已经落后了很多term了，则它的logs也属于落后的
	if rf.currentTerm > args.Term {
		DPrintf(rf.me, "AppendEntries %v<-%v currentTerm %v > term %v", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 这里不像RequestVote必须要求args.Term > rf.currentTerm，因为AppendEntries会作为heartbeats保持
	// 所以rf.currentTerm需要保持和leader发来的args.Term一致
	if rf.currentTerm == args.Term {
		// Do nothing 正常情况
	}

	// Tips 如果一个非follower状态的server，走到了这一步，说明集群中出现了 更新的server
	// 则它要立即 revert to follower
	if rf.status != follower {
		DPrintf(rf.me, "AppendEntries %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately",
			rf.me, args.LeaderId, rf.status, rf.currentTerm, args.Term)
		rf.status = follower
		rf.currentTerm = args.Term
		DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v, logs %v", rf.me, rf.status, rf.currentTerm, rf.log)
	}

	// consistency check：prevLogIndex所在的log entry，它的term不等于prevLogTerm。
	passCheck := true
	for loop := true; loop; loop = false {
		// 原因：虽然follower认为已经committed的log，但整个集群并不认为，所以每次需要leader的overwrite
		lastCommittedLogIndex, _ := rf.getLastCommittedLogIndexTerm()
		if lastCommittedLogIndex < args.PrevLogIndex {
			// 防止slice越界
			passCheck = false
			DPrintf(rf.me, "AppendEntries %v<-%v fail consistency for index. %v < %v",
				rf.me, args.LeaderId, lastCommittedLogIndex, args.PrevLogIndex)
			break
		}

		matchedIndexLogTerm := rf.getLogEntry(args.PrevLogIndex).Term
		if matchedIndexLogTerm != args.PrevLogTerm {
			passCheck = false
			DPrintf(rf.me, "AppendEntries %v<-%v fail consistency for term. %v != %v",
				rf.me, args.LeaderId, matchedIndexLogTerm, args.PrevLogTerm)
			break
		}
	}

	reply.Term = rf.currentTerm
	if passCheck {
		DPrintf(rf.me, "AppendEntries %v<-%v pass consistency check", rf.me, args.LeaderId)
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
		if args.LeaderCommit > rf.committedIndex {
			// 存在一种情况，follower落后leader很多，这次appendEntries还未补全所有log，
			// 所以 这次follower的committedIndex为最后一个logIndex
			rf.committedIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		}
	} else {
		reply.Success = false
	}

	rf.resetElectionSignal <- struct{}{}
	rf.resetAppendEntriesSignal <- struct{}{}
	return
}
