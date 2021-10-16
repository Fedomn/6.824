package raft

import (
	"fmt"
	"math/rand"
)

func (rf *Raft) Step(e Event) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		if e.DoneC != nil {
			close(e.DoneC)
		}
	}()

	switch e.Type {
	case EventVote, EventPreVote:
		args := e.Args.(*RequestVoteArgs)
		reply := e.Reply.(*RequestVoteReply)
		if args.Seq <= rf.recvRpcLatestSeq[e.From] {
			DRpcPrintf(rf.me, args.Seq, "%s %v<-%v AbortOldRPC argsSeq:%v <= recvRpcLatestSeq:%v, ignore it", e.Type, rf.me, args.CandidateId, args.Seq, rf.recvRpcLatestSeq[e.From])
			return nil
		}
		rf.setRecvRpcLatestSeq(e.From, args.Seq)

		switch {
		case args.Term < rf.currentTerm:
			DRpcPrintf(rf.me, args.Seq, "%s %v<-%v currentTerm %v > term %v, ignore lower term", e.Type, rf.me, args.CandidateId, rf.currentTerm, args.Term)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		case args.Term == rf.currentTerm:
			if rf.votedFor != None && rf.votedFor != args.CandidateId {
				DRpcPrintf(rf.me, args.Seq, "%s %v<-%v currentTerm %v == term %v, already votedFor %v", e.Type, rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.votedFor)
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			} else {
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				DRpcPrintf(rf.me, args.Seq, "%s %v<-%v currentTerm %v == term %v, haven't vote, will votedFor %v", e.Type, rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.votedFor)
			}
		case args.Term > rf.currentTerm:
			passCheck := true
			for loop := true; loop; loop = false {
				lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
				if args.LastLogTerm < lastLogTerm {
					passCheck = false
					DRpcPrintf(rf.me, args.Seq, "%s %v<-%v fail election restriction check about term. %v < %v", e.Type, rf.me, args.CandidateId, args.LastLogTerm, lastLogTerm)
					break
				}

				if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
					passCheck = false
					DRpcPrintf(rf.me, args.Seq, "%s %v<-%v fail election restriction check about index when same term. %v < %v", e.Type, rf.me, args.CandidateId, args.LastLogIndex, lastLogIndex)
					break
				}
			}

			// 注意preVote不会影响raft状态
			if e.Type == EventPreVote {
				reply.VoteGranted = passCheck
				reply.Term = rf.currentTerm
				DRpcPrintf(rf.me, args.Seq, "%s %v<-%v reply:%+v", e.Type, rf.me, args.CandidateId, reply)
				break
			}

			originalCurrentTerm := rf.currentTerm
			rf.currentTerm = args.Term
			if passCheck {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				reply.Term = rf.currentTerm
			} else {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
			}

			if rf.state != StateFollower {
				DRpcPrintf(rf.me, args.Seq, "%s %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately", e.Type, rf.me, args.CandidateId, rf.state, originalCurrentTerm, reply.Term)
				rf.becomeFollower(args.Term, None)
			}
		}
		if reply.VoteGranted {
			rf.electionElapsed = 0
		}
		return nil
	case EventApp:
		args := e.Args.(*AppendEntriesArgs)
		reply := e.Reply.(*AppendEntriesReply)
		if args.Seq <= rf.recvRpcLatestSeq[e.From] {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v AbortOldRPC argsSeq:%v <= recvRpcLatestSeq:%v, ignore it", rf.me, args.LeaderId, args.Seq, rf.recvRpcLatestSeq[e.From])
			return nil
		}
		rf.setRecvRpcLatestSeq(e.From, args.Seq)

		switch {
		case args.Term < rf.currentTerm:
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v currentTerm %v > term %v, ignore lower term", rf.me, args.LeaderId, rf.currentTerm, args.Term)
			reply.Term = rf.currentTerm
			reply.Success = false
		case args.Term >= rf.currentTerm:
			if rf.state != StateFollower {
				DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately", rf.me, args.LeaderId, rf.state, rf.currentTerm, args.Term)
				rf.becomeFollower(args.Term, args.LeaderId)
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
					// 注意，conflict index应该为最后的index + 1，因为在下一次RPC需要计算PrevLogIndex=nextIndex-1
					reply.ConflictIndex = lastLogIndex + 1
					DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v fail consistency for lastLogIndex. %v < %v, conflictIndex:%v", rf.me, args.LeaderId, lastLogIndex, args.PrevLogIndex, reply.ConflictIndex)
					break
				}

				// 已经snapshot后，prevLogIndex在snapshot之前
				firstLogIndex := rf.getFirstLogIndex()
				if args.PrevLogIndex < firstLogIndex {
					passCheck = false
					reply.ConflictIndex = firstLogIndex + 1
					DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v fail consistency for firstLogIndex. %v > %v, conflictIndex:%v", rf.me, args.LeaderId, firstLogIndex, args.PrevLogIndex, reply.ConflictIndex)
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
					DRpcPrintf(rf.me, args.Seq, "AppendEntries %v<-%v fail consistency for term. %v != %v, conflictIndex:%v", rf.me, args.LeaderId, matchedIndexLogTerm, args.PrevLogTerm, reply.ConflictIndex)
					break
				}
			}
			if passCheck {
				rf.currentTerm = args.Term
				reply.Term = rf.currentTerm
				reply.Success = true
				reply.NextIndex = args.PrevLogIndex + 1 + len(args.Entries)

				newLog := make([]LogEntry, 0)
				for _, entry := range args.Entries {
					logEntry := LogEntry{
						Command: entry.Command,
						Term:    entry.Term,
						Index:   entry.Index,
					}
					newLog = append(newLog, logEntry)
				}
				rf.log = append(rf.log[:rf.getLogEntryIndex(args.PrevLogIndex)+1], newLog...)

				// 如果leader commit index > follower 本地存储的 commit index，
				// 则更新 follower本地的 commitIndex = min(leaderCommit , 将要保存的logs中最后一个log entry index)
				if args.LeaderCommit > rf.commitIndex {
					// 存在一种情况，follower落后leader很多，这次appendEntries还未补全所有log，
					// 所以 这次follower的committedIndex为最后一个logIndex
					rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())

					// 真正的异步apply在 asyncApplier
					rf.applyCond.Signal()
				}
			} else {
				reply.Success = false
				reply.Term = rf.currentTerm
			}
		}
		if reply.Success {
			rf.electionElapsed = 0
			rf.lead = args.LeaderId
		}
		return nil
	case EventSnap:
		args := e.Args.(*InstallSnapshotArgs)
		reply := e.Reply.(*InstallSnapshotReply)
		DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v receiveEventSnap args:%v", rf.me, args.LeaderId, args)
		if args.Seq <= rf.recvRpcLatestSeq[e.From] {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v AbortOldRPC argsSeq:%v <= recvRpcLatestSeq:%v, ignore it", rf.me, args.LeaderId, args.Seq, rf.recvRpcLatestSeq[e.From])
			return nil
		}
		rf.setRecvRpcLatestSeq(e.From, args.Seq)

		switch {
		case args.Term < rf.currentTerm:
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v currentTerm %v > term %v, ignore lower term", rf.me, args.LeaderId, rf.currentTerm, args.Term)
			reply.Term = rf.currentTerm
			reply.Success = false
		case args.Term >= rf.currentTerm:
			if rf.state != StateFollower {
				DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v<-%v %s currentTerm %v got higher term %v, so revert to follower immediately", rf.me, args.LeaderId, rf.state, rf.currentTerm, args.Term)
				rf.becomeFollower(args.Term, args.LeaderId)
			}
			// 这里不需要consistency check，因为一致性检测的结果是为了计算conflictIndex，让leader快速设置nextIndex，继续下一层appendEntries RPC
			// 但对于InstallSnapshot来说，不需要conflictIndex，因为follower要无条件接受leader的snapshot
			reply.Term = args.Term
			reply.Success = true

			// 这里apply snapshot，等待上层CondInstallSnapshot成功后，apply到应用层
			rf.asyncSnapshotCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
		}
		if reply.Success {
			rf.electionElapsed = 0
			rf.lead = args.LeaderId
		}
		return nil
	default:
		return rf.step(rf, e)
	}
}

func stepFollower(rf *Raft, e Event) error {
	switch e.Type {
	case EventPreHup:
		rf.becomePreCandidate()
		rf.send(e)
	default:
		DPrintf(rf.me, fmt.Sprintf("ignore event:%v for %v", e, rf.state))
	}
	return nil
}

func stepPreCandidate(rf *Raft, e Event) error {
	switch e.Type {
	case EventPreHup:
		rf.startPreRequestVote()
	case EventPreVoteResp:
		args := e.Args.(*RequestVoteArgs)
		reply := e.Reply.(*RequestVoteReply)
		if args.Seq != rf.sendRpcLatestSeq[e.From].SendSeq {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v AbortOldRPC argsSeq:%v != sendRpcLatestSeq:%v, ignore it", e.From, e.To, args.Seq, rf.sendRpcLatestSeq[e.From].SendSeq)
			return nil
		}
		rf.sendRpcLatestSeq[e.From].RecvSeq = args.Seq

		if reply == nil {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v gotRPC nil, reply:%v", e.From, e.To, reply)
			return nil
		}
		if reply.Term > rf.currentTerm {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.VoteGranted {
			rf.voteFrom[e.From] = struct{}{}
		}
		// got majority votes
		if len(rf.voteFrom) >= len(rf.peers)/2+1 {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v voteFrom %+v", e.From, e.To, rf.voteFrom)
			rf.becomeCandidate()
			rf.send(Event{Type: EventHup, From: rf.me, To: rf.me, Term: rf.currentTerm})
			return nil
		}
	default:
		DPrintf(rf.me, fmt.Sprintf("ignore event:%v for raft:%v", e, rf.state))
	}
	return nil
}

func stepCandidate(rf *Raft, e Event) error {
	switch e.Type {
	case EventHup:
		rf.startRequestVote()
	case EventVoteResp:
		args := e.Args.(*RequestVoteArgs)
		reply := e.Reply.(*RequestVoteReply)
		if args.Seq != rf.sendRpcLatestSeq[e.From].SendSeq {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v AbortOldRPC argsSeq:%v != sendRpcLatestSeq:%v, ignore it", e.From, e.To, args.Seq, rf.sendRpcLatestSeq[e.From].SendSeq)
			return nil
		}
		rf.sendRpcLatestSeq[e.From].RecvSeq = args.Seq

		if reply == nil {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v gotRPC nil, reply:%v", e.From, e.To, reply)
			return nil
		}
		if reply.Term > rf.currentTerm {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.VoteGranted {
			rf.voteFrom[e.From] = struct{}{}
		}
		// got majority votes
		if len(rf.voteFrom) >= len(rf.peers)/2+1 {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v voteFrom %+v", e.From, e.To, rf.voteFrom)
			rf.becomeLeader()
			//rf.log = append(rf.log, LogEntry{
			//	Command: "NoOp",
			//	Term:    rf.currentTerm,
			//	Index:   rf.getLastLogIndex() + 1,
			//})
			rf.send(Event{Type: EventBeat, From: rf.me, Term: rf.currentTerm})
			return nil
		}
		// split vote的情况处理逻辑，发生在electionTimeout里，避免这里还需要计数requestedVoteCount
		// 同时我们基于event-driven后，channel是顺序取event，就出现了requestedVoteCount已经=3了，但状态机还未收到event3
	default:
		DPrintf(rf.me, fmt.Sprintf("ignore event:%v for raft:%v", e, rf.state))
	}
	return nil
}

func stepLeader(rf *Raft, e Event) error {
	switch e.Type {
	case EventBeat:
		rf.startAppendEntries(true)
	case EventReplicate:
		rf.startAppendEntries(false)
	case EventAppResp:
		args := e.Args.(*AppendEntriesArgs)
		reply := e.Reply.(*AppendEntriesReply)
		if args.Seq <= rf.sendRpcLatestSeq[e.From].RecvSeq {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v AbortOldRPC argsSeq:%v <= recvRpcLatestSeq:%v, ignore it", e.From, e.To, args.Seq, rf.sendRpcLatestSeq[e.From].RecvSeq)
			return nil
		}
		rf.sendRpcLatestSeq[e.From].RecvSeq = args.Seq

		if reply == nil {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v gotRPC nil, reply:%v", e.From, e.To, reply)
			return nil
		}
		if reply.Term > rf.currentTerm {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.Success {
			if len(args.Entries) == 0 {
				DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v gotRPC success, entries len: %v, so heartbeat will do nothing", e.From, e.To, len(args.Entries))
			} else {
				rf.setNextIndexAndMatchIndexDirectly(e.From, reply.NextIndex)
				DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v gotRPC success, entries len: %v, so had increased nextIndex %v, matchIndex %v", e.From, e.To, len(args.Entries), rf.nextIndex, rf.matchIndex)
			}
		} else {
			rf.nextIndex[e.From] = reply.ConflictIndex
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v gotRPC false, so will decrease nextIndex and append again, %v", e.From, e.To, rf.nextIndex)
			return nil
		}

		// maybeCommit
		calcCommitIndex := rf.calcCommitIndex()
		if rf.commitIndex != calcCommitIndex {
			DRpcPrintf(rf.me, args.Seq, "AppendEntries %v->%v leader maybe commit, before:%v after calc commitIndex:%v, matchIndex: %v", e.From, e.To, rf.commitIndex, calcCommitIndex, rf.matchIndex)
			rf.commitIndex = calcCommitIndex

			// 真正的异步apply在 asyncApplier
			rf.applyCond.Signal()
		}
	case EventSnapResp:
		args := e.Args.(*InstallSnapshotArgs)
		reply := e.Reply.(*InstallSnapshotReply)
		if args.Seq != rf.sendRpcLatestSeq[e.From].SendSeq {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v AbortOldRPC argsSeq:%v != sendRpcLatestSeq:%v, ignore it", e.From, e.To, args.Seq, rf.sendRpcLatestSeq[e.From].SendSeq)
			return nil
		}
		rf.sendRpcLatestSeq[e.From].RecvSeq = args.Seq

		if reply == nil {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v gotRPC nil, reply:%v", e.From, e.To, reply)
			return nil
		}
		if reply.Term > rf.currentTerm {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v currentTerm %v got higher term %v, so revert to follower immediately", e.From, e.To, rf.currentTerm, reply.Term)
			rf.becomeFollower(reply.Term, None)
			return nil
		}
		if reply.Success {
			rf.setNextIndexAndMatchIndexAfterSnapshot(e.From)
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v gotRPC success, so set nextIndex %v, matchIndex %v", e.From, e.To, rf.nextIndex, rf.matchIndex)
		} else {
			DRpcPrintf(rf.me, args.Seq, "InstallSnapshot %v->%v gotRPC false, so do nothing and install again, %v", e.From, e.To)
			return nil
		}
	default:
		DPrintf(rf.me, fmt.Sprintf("ignore event:%v for raft:%v", e, rf.state))
	}
	return nil
}

func (rf *Raft) setNextIndexAndMatchIndexAfterSnapshot(peerIdx int) {
	rf.nextIndex[peerIdx] = rf.getFirstLogIndex() + 1
	rf.matchIndex[peerIdx] = rf.nextIndex[peerIdx] - 1
}

func (rf *Raft) becomeFollower(term int, lead int) {
	rf.reset(term)
	rf.state = StateFollower
	rf.tick = rf.tickElection
	rf.step = stepFollower
	rf.lead = lead
	DPrintf(rf.me, "Raft %v became follower at term %v", rf.me, rf.currentTerm)
	rf.persist()
}

func (rf *Raft) becomePreCandidate() {
	if rf.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// 注意这里：preCandidate term是不会递增的
	rf.reset(rf.currentTerm)
	rf.state = StatePreCandidate
	rf.votedFor = rf.me
	rf.voteFrom[rf.me] = struct{}{}
	rf.tick = rf.tickElection
	rf.step = stepPreCandidate
	DPrintf(rf.me, "Raft %v became pre-candidate at term %v", rf.me, rf.currentTerm)
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	if rf.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	rf.reset(rf.currentTerm + 1)
	rf.state = StateCandidate
	rf.votedFor = rf.me
	rf.voteFrom[rf.me] = struct{}{}
	rf.tick = rf.tickElection
	rf.step = stepCandidate
	DPrintf(rf.me, "Raft %v became candidate at term %v", rf.me, rf.currentTerm)
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	if rf.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	rf.reset(rf.currentTerm)
	rf.state = StateLeader
	rf.tick = rf.tickHeartbeat
	rf.step = stepLeader
	rf.lead = rf.me

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}

	DPrintf(rf.me, "Raft %v became leader at term %v, "+
		"commitIndex:%v, lastApplied:%v, nextIndex:%v, matchIndex:%v, Last3Logs:%v",
		rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, debugLast3Logs(rf.log))
	rf.persist()
}

func (rf *Raft) tickElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionElapsed++

	if rf.electionElapsed >= rf.randomizedElectionTimeout {
		DPrintf(rf.me, "tick election timeout !")

		if rf.state == StateCandidate || rf.state == StatePreCandidate {
			DPrintf(rf.me, "%s %v start election again, may encounter split vote at term %v !", rf.state, rf.me, rf.currentTerm)
			rf.becomeFollower(rf.currentTerm, None)
		}

		rf.electionElapsed = 0
		rf.send(Event{Type: EventPreHup, From: rf.me, Term: rf.currentTerm})
	}
}

func (rf *Raft) tickHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeatElapsed++
	rf.electionElapsed++

	if rf.state != StateLeader {
		return
	}

	if rf.heartbeatElapsed >= rf.heartbeatTimeout {
		DPrintf(rf.me, "tick heartbeat timeout !")
		rf.heartbeatElapsed = 0
		rf.send(Event{Type: EventBeat, From: rf.me, Term: rf.currentTerm})
	}
}

func (rf *Raft) reset(term int) {
	if rf.currentTerm != term {
		rf.currentTerm = term
		rf.votedFor = None
	}
	rf.lead = None
	rf.voteFrom = make(map[int]struct{})

	rf.electionElapsed = 0
	rf.heartbeatElapsed = 0
	rf.randomizedElectionTimeout = rf.electionTimeout + rand.Intn(rf.electionTimeout)
}
