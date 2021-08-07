package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type raftStatus int

const (
	follower raftStatus = iota
	candidate
	leader
)

func (r raftStatus) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	default:
		return "unknown"
	}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	status raftStatus

	currentTerm int
	votedFor    int        // voted for candidate's id
	log         []LogEntry // first index is 1

	committedIndex int
	lastApplied    int

	// for leader
	nextIndex  []int // 每个peer一个，为leader下次发送的log entry index
	matchIndex []int // 每个peer一个，为leader已经复制的highest log entry index

	// for election internal use
	resetElectionSignal   chan struct{}
	requestVoteCnt        int
	requestVoteGrantedCnt int

	// for crash and rejoins server 一旦unhealthy，它就不能参与投票 for RequestVote和AppendEntries
	peersHealthStatus map[int]bool
}

type LogEntry struct {
	command interface{}
	term    int
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
		rf.status = follower
		DPrintf(rf.me, "AppendEntries %v<-%v revert to follower", rf.me, args.LeaderId)
	}

	rf.resetElectionSignal <- struct{}{}

	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startRequestVote(ctx context.Context) {
	if rf.getStatusWithLock() == leader {
		return
	}

	rf.safe(func() {
		// reset 计数器
		rf.requestVoteCnt = 0
		rf.requestVoteGrantedCnt = 0
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
				// 如果已经在其它goroutine变成了follower，这里就不在处理
				if rf.getStatusWithLock() == follower && rf.currentTerm >= reply.Term {
					return
				}
				DPrintf(rf.me, "RequestVote %v->%v currentTerm %v got higher term %v, so revert to follower immediately", rf.me, peerIdx, rf.getCurrentTermWithLock(), reply.Term)
				rf.mu.Lock()
				// set currentTerm的目的：明知道当前这个server的term已经落后于集群了，需要尽早追赶上，就直接赋值成reply的term
				rf.currentTerm = reply.Term
				rf.status = follower
				DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v", rf.me, rf.status, rf.currentTerm)
				// rf.resetElectionSignal <- struct{}{}
				rf.mu.Unlock()
				return
			}

			voteGrantedCnt := 0
			if reply.VoteGranted {
				rf.mu.Lock()
				rf.requestVoteGrantedCnt++
				voteGrantedCnt = rf.requestVoteGrantedCnt
				rf.mu.Unlock()
			}

			rf.mu.Lock()
			unhealthyCount := 0
			for _, isHealthy := range rf.peersHealthStatus {
				if !isHealthy {
					unhealthyCount++
				}
			}
			rf.mu.Unlock()
			majorityCount := (len(rf.peers) - unhealthyCount) / 2

			// to fix if there's no quorum, no leader should be elected.
			if majorityCount == 0 {
				DPrintf(rf.me, "RequestVote %v->%v no quorum, no leader should be elected", rf.me, peerIdx)
				return
			}

			isEven := (len(rf.peers)-unhealthyCount)%2 == 0 // 判断是否是偶数，如果是的话 voteGrantedCnt>=majorityCount，否则>
			if (isEven && voteGrantedCnt >= majorityCount) || (!isEven && voteGrantedCnt > majorityCount) {
				if rf.isLeaderWithLock() {
					DPrintf(rf.me, "RequestVote %v->%v already leader do nothing", rf.me, peerIdx)
					return
				}
				if rf.isCandidateWithLock() {
					DPrintf(rf.me, "RequestVote %v->%v got majority votes, so upgrade to leader immediately", rf.me, peerIdx)
					rf.setStatusWithLock(leader) // send heartbeat immediately
					// TODO not sure this code is right
					go rf.startAppendEntries(context.Background())
				}
				return
			}

			// 走到这里说明 已经RequestVote给到了majority的server，但没有得到voteGrant，所以增加term，再开始election
			// 注意：此时会revert to follower，等待election timeout在变成candidate
			if (isEven && voteCnt >= majorityCount) || (!isEven && voteCnt > majorityCount) {
				// 如果已经在其它goroutine变成了follower，这里就不在处理
				if rf.isFollower(true) {
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
				// 如果已经在其它goroutine变成了follower，这里就不在处理
				if rf.getStatusWithLock() == follower && rf.currentTerm >= reply.Term {
					return
				}
				DPrintf(rf.me, "AppendEntries %v->%v currentTerm %v got higher term %v, so revert to follower immediately", rf.me, peerIdx, rf.getCurrentTermWithLock(), reply.Term)
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.status = follower
				DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v", rf.me, rf.status, rf.currentTerm)
				rf.resetElectionSignal <- struct{}{}
				rf.mu.Unlock()
				return
			}

			if reply.Success {
				// TODO
			}
		}()
	}
}

// The ticker goroutine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {
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

// The ticker2 goroutine to send appendEntries RPC when current status is leader
func (rf *Raft) ticker2() {
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
			if rf.getStatusWithLock() != leader {
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

func (rf *Raft) getElectionSleepTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randomSleepTime := rand.Intn(electionTimeoutRange[1]-electionTimeoutRange[0]) + electionTimeoutRange[0]
	sleepDuration := time.Duration(randomSleepTime) * time.Millisecond
	return sleepDuration
}

func (rf *Raft) String() string {
	return fmt.Sprintf(
		"status:%d, currentTerm:%d",
		rf.status, rf.currentTerm)
}

func (rf *Raft) getCurrentTermWithLock() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) setStatusWithLock(status raftStatus) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = status
	DPrintf(rf.me, "Raft %v convert to %s, currentTerm %v", rf.me, status, rf.currentTerm)
}

func (rf *Raft) getStatusWithLock() raftStatus {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status
}

func (rf *Raft) getLastLogIndexTerm() (int, int) {
	lastLogIndex, lastLogTerm := 0, 0
	if len(rf.log) != 0 {
		lastLogIndex = len(rf.log)
		lastLogTerm = rf.log[lastLogIndex-1].term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) isLeaderWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == leader
}

func (rf *Raft) isCandidateWithLock() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == candidate
}

func (rf *Raft) isFollower(lock bool) bool {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	return rf.status == follower
}

func (rf *Raft) safe(fun func()) {
	rf.mu.Lock()
	fun()
	rf.mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.committedIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.resetElectionSignal = make(chan struct{})
	rf.peersHealthStatus = make(map[int]bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start ticker goroutine to start sendAppendEntries RPC when
	go rf.ticker2()

	return rf
}
