package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// The RequestVoteTicker goroutine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) RequestVoteTicker() {
	var rpcMutex sync.Mutex
	var lastRpcCancel context.CancelFunc

	// 开始election timeout的循环，timeout在减少的过程中，某个时刻timeout可能会被reset，则重新开始election timeout
	tickerIsUp := make(chan struct{})
	tickerCtx, tickerCancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-time.After(rf.getElectionSleepTime()):
				tickerIsUp <- struct{}{}
				continue
			case <-rf.resetElectionSignal:
				DPrintf(rf.me, "Raft: %+v will reset election timeout", rf.me)
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
		select {
		case <-tickerIsUp:
			rpcCtx, rpcCancel := context.WithCancel(context.Background())
			if lastRpcCancel != nil {
				lastRpcCancel()
			}
			rpcMutex.Lock()
			lastRpcCancel = rpcCancel // 第一次初始化 + 第n次赋值
			rpcMutex.Unlock()
			go rf.startRequestVote(rpcCtx)
			continue
		}
	}

	tickerCancel()
	if lastRpcCancel != nil {
		lastRpcCancel()
	}
}

func (rf *Raft) startRequestVote(ctx context.Context) {
	if rf.getStatusWithLock() == leader {
		return
	}

	// reset 计数器
	rf.safe(func() {
		// include candidate itself
		rf.requestVoteCnt = 1
		rf.requestVoteGrantedCnt = 1
	})

	// Step 1: 准备election需要数据
	rf.safe(func() {
		rf.currentTerm++
		rf.status = candidate
		rf.votedFor = rf.me
	})

	// Step 2: 发送RequestVote RPC 并根据reply决定是升级leader还是降为follower
	for idx := range rf.peers {
		if idx == rf.me { // ignore itself
			continue
		}
		peerIdx := idx

		// Tips 注意：从这开始是 多个goroutine 并发修改状态，可能存在时序问题，所以每次操作前 确保前置条件正确
		go func() {
			lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
			args := &RequestVoteArgs{
				Term:         rf.getCurrentTermWithLock(),
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			if !rf.isCandidateWithLock() {
				return
			}
			// RPC请求存在delay或hang住情况
			rpcDone := make(chan bool, 1)
			go func() {
				DPrintf(rf.me, "RequestVote %v->%v send RPC %+v", rf.me, peerIdx, args)
				if ok := rf.sendRequestVote(peerIdx, args, reply); !ok {
					DPrintf(rf.me, "RequestVote %v->%v RPC got ok false", rf.me, peerIdx)
				}
				rpcDone <- true
			}()

			select {
			case <-ctx.Done():
				rf.safe(func() {
					// election timeout到了，忽略掉当前RPC的reply，直接进入下一轮
					DPrintf(rf.me, "RequestVote %v->%v election timeout, start next election and mark unhealthy", rf.me, peerIdx)
					// 因为可能已经有voteGrant的server，并且这些server已经更新了它们的currentTerm=args.Term
					// 所以为了让下一次election成功，candidate必须要让自己的term+1
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

			DPrintf(rf.me, "RequestVote %v->%v RPC got %+v %+v", rf.me, peerIdx, reply, args)

			voteCnt := 0
			rf.safe(func() {
				rf.requestVoteCnt++
				voteCnt = rf.requestVoteCnt
			})

			// 只要有一个follower的term给candidate大，立即revert to follower
			// 注意：这里rf.getCurrentTerm可能会被其它goroutine修改到，比如rejoin的sever的term更大，它的RPC会将server term修改掉
			if reply.Term > rf.getCurrentTermWithLock() && reply.VoteGranted == false {
				if rf.isLeaderWithLock() && rf.currentTerm >= reply.Term { // 如果已经在其它goroutine变成了follower，这里就不在处理
					return
				}
				DPrintf(rf.me, "RequestVote %v->%v currentTerm %v got higher term %v, so revert to follower immediately", rf.me, peerIdx, rf.getCurrentTermWithLock(), reply.Term)
				rf.safe(func() {
					// set currentTerm的目的：明知道当前这个server的term已经落后于集群了，需要尽早追赶上，就直接赋值成reply的term
					rf.currentTerm = reply.Term
					rf.status = follower
					DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v", rf.me, rf.status, rf.currentTerm)
					// FIXME
					rf.resetElectionSignal <- struct{}{}
				})
				return
			}

			voteGrantedCnt := 0
			if reply.VoteGranted {
				rf.safe(func() {
					rf.requestVoteGrantedCnt++
					voteGrantedCnt = rf.requestVoteGrantedCnt
				})
			}

			majorityCount := len(rf.peers)/2 + 1

			if voteGrantedCnt >= majorityCount {
				if rf.isLeaderWithLock() {
					DPrintf(rf.me, "RequestVote %v->%v already leader do nothing", rf.me, peerIdx)
					return
				}
				if rf.isCandidateWithLock() {
					DPrintf(rf.me, "RequestVote %v->%v got majority votes, so upgrade to leader immediately", rf.me, peerIdx)
					rf.setStatusWithLock(leader) // send heartbeat immediately
					go rf.startAppendEntries(context.Background())
				}
				return
			}

			// 走到这里说明 已经RequestVote给到了majority的server，但没有得到voteGrant，所以增加term，再开始election
			// 注意：此时会revert to follower，等待election timeout在变成candidate
			if voteCnt >= majorityCount {
				// 如果已经在其它goroutine变成了follower，这里就不在处理
				if rf.isFollowerWithLock() {
					return
				}
				rf.safe(func() {
					DPrintf(rf.me, "RequestVote %v->%v may encounter split vote, so revert to follower and wait next election", rf.me, peerIdx)
					rf.status = follower
					DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v", rf.me, rf.status, rf.currentTerm)
					rf.resetElectionSignal <- struct{}{}
				})
				return
			}
		}()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer DPrintf(rf.me, "RequestVote %v<-%v reply %+v %+v", rf.me, args.CandidateId, reply, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 异常情况：follower term > candidate term，说明candidate已经在集群中落后了，返回false
	// 比如：一个follower刚从crash中recover，但它已经落后了很多term了，则它的logs也属于落后的
	if rf.currentTerm > args.Term {
		DPrintf(rf.me, "RequestVote %v<-%v currentTerm %v > term %v", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// edge case：比如，上一个term的follower在当前term才从crash中recover，则它刚好start election，将上一个term+1，就是当前这个Raft函数
	// 这时有个candidate向它发送RequestVote，刚好term相等。则也不应该
	// 或者 比如：刚好2个follower在同一时间start election，互相发送了RequestVote RPC，这种情况下不应该 vote
	if rf.currentTerm == args.Term && rf.votedFor != args.CandidateId {
		DPrintf(rf.me, "RequestVote %v<-%v currentTerm %v == term %v, candidate not same as votedFor %v", rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 正常情况：follower term < candidate term，说明candidate早于follower，再经过一致性检查通过后，返回true

	// consistency check
	passCheck := false
	for loop := true; loop; loop = false {
		if len(rf.log) == 0 && rf.votedFor == -1 { // first vote returns true
			passCheck = true
			break
		}

		if len(rf.log) == 0 && args.LastLogIndex == 0 { // not start append any entries
			passCheck = true
			break
		}

		// 基于logIndex从1开始计算
		if len(rf.log) == args.LastLogIndex && rf.log[args.LastLogIndex-1].term == args.LastLogTerm {
			passCheck = true
			break
		}
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	if passCheck {
		DPrintf(rf.me, "RequestVote %v<-%v pass consistency check", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

		// Tips 如果一个非follower状态的server，走到了这一步，说明集群中出现了 更新的server
		// 则它要立即 revert to follower
		if rf.status != follower {
			rf.status = follower
			DPrintf(rf.me, "RequestVote %v<-%v revert to follower", rf.me, args.CandidateId)
		}

		// Tips 在grant vote后，需要立即reset自己的election timeout，防止leader还未发送heartbeats，自己election timeout到了
		// 从而导致 higher term会在下次 election中当选
		rf.resetElectionSignal <- struct{}{}
	} else {
		DPrintf(rf.me, "RequestVote %v<-%v fail consistency check", rf.me, args.CandidateId)
	}
	return
}

// helper functions
func (rf *Raft) getElectionSleepTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randomSleepTime := rand.Intn(electionTimeoutRange[1]-electionTimeoutRange[0]) + electionTimeoutRange[0]
	sleepDuration := time.Duration(randomSleepTime) * time.Millisecond
	return sleepDuration
}
