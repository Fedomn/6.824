package raft

func (rf *Raft) startRequestVote() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		peerIdx := idx
		lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
		args := &RequestVoteArgs{
			Seq:          rf.incSendRpcLatestSendSeq(peerIdx),
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}
		go func() {
			DRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v sendRPC %+v", rf.me, peerIdx, args)
			if ok := rf.peers[peerIdx].Call("Raft.RequestVote", args, reply); !ok {
				if !rf.killed() {
					TRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v RPC not reply", rf.me, peerIdx)
				}
			} else {
				if reply.Abort {
					TRpcPrintf(rf.me, args.Seq, "RequestVote %v->%v abortReply", rf.me, peerIdx)
					return
				}
				rf.send(Event{Type: EventVoteResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
			}
		}()
	}
}

func (rf *Raft) startPreRequestVote() {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		peerIdx := idx
		lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
		args := &RequestVoteArgs{
			Seq: rf.incSendRpcLatestSendSeq(peerIdx),
			// 注意这里：preCandidate请求模拟term+1的情况，如果通过，后面becomeCandidate后，term+1自然也会成功，
			// 而follower那边对preRequestVote，不会覆盖自身term
			Term:         rf.currentTerm + 1,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}
		go func() {
			DRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v sendRPC %+v", rf.me, peerIdx, args)
			if ok := rf.peers[peerIdx].Call("Raft.RequestPreVote", args, reply); !ok {
				if !rf.killed() {
					TRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v RPC not reply", rf.me, peerIdx)
				}
			} else {
				if reply.Abort {
					TRpcPrintf(rf.me, args.Seq, "RequestPreVote %v->%v abortReply", rf.me, peerIdx)
					return
				}
				rf.send(Event{Type: EventPreVoteResp, From: peerIdx, To: rf.me, Term: args.Term, Args: args, Reply: reply})
			}
		}()
	}
}

func (rf *Raft) RequestPreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.sendWait(Event{Type: EventPreVote, From: args.CandidateId, To: rf.me, Term: args.Term, Args: args, Reply: reply, DoneC: make(chan struct{})})
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// labrpc这里的调用到这里后，同步等待方法执行完，再返回reply
	rf.sendWait(Event{Type: EventVote, From: args.CandidateId, To: rf.me, Term: args.Term, Args: args, Reply: reply, DoneC: make(chan struct{})})
}
