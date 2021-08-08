package raft

import (
	"context"
	"time"
)

// The AppendEntriesTicker goroutine to send appendEntries RPC when current status is leader
func (rf *Raft) AppendEntriesTicker() {
	// 开始AppendEntries RPC(也是heartbeats) 循环
	tickerIsUp := make(chan struct{})
	tickerCtx, tickerCancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-time.After(heartbeatsTimeout * time.Millisecond):
				tickerIsUp <- struct{}{}
				continue
			case <-tickerCtx.Done():
				return
			}
		}
	}()

	var lastRpcCancel context.CancelFunc
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
			lastRpcCancel = rpcCancel // 第一次初始化 + 第n次赋值
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

	for idx := range rf.peers {
		if idx == rf.me { // ignore itself
			continue
		}
		peerIdx := idx
		go func() {
			args := &AppendEntriesArgs{
				Term:         rf.getCurrentTermWithLock(),
				LeaderId:     rf.me,
				PervLogIndex: -1,
				PrevLogTerm:  -1,
				Entries:      make([]interface{}, 0),
				LeaderCommit: -1,
			}
			reply := &AppendEntriesReply{}

			if !rf.isLeaderWithLock() {
				return
			}
			// RPC请求存在delay或hang住情况
			rpcDone := make(chan bool, 1)
			go func() {
				DPrintf(rf.me, "AppendEntries %v->%v send RPC %+v", rf.me, peerIdx, args)
				if ok := rf.sendAppendEntries(peerIdx, args, reply); !ok {
					DPrintf(rf.me, "AppendEntries %v->%v RPC got ok false", rf.me, peerIdx)
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

			if reply.Term > rf.getCurrentTermWithLock() && reply.Success == false {
				if rf.isFollowerWithLock() { // 如果已经在其它goroutine变成了follower，这里就不在处理
					return
				}
				DPrintf(rf.me, "AppendEntries %v->%v %s currentTerm %v got higher term %v, so revert to follower immediately",
					rf.me, peerIdx, rf.getStatusWithLock(), rf.getCurrentTermWithLock(), reply.Term)
				rf.safe(func() {
					rf.currentTerm = reply.Term
					rf.status = follower
					DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v", rf.me, rf.status, rf.currentTerm)

					rf.resetElectionSignal <- struct{}{}
				})
				return
			}

			if reply.Success {
				// TODO
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
		DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v", rf.me, rf.status, rf.currentTerm)
	}

	rf.resetElectionSignal <- struct{}{}

	reply.Term = rf.currentTerm
	reply.Success = true
	return
}
