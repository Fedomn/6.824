package v2

import (
	"6.824/labrpc"
	"time"
)

type Node struct {
	raft *Raft
	tick *time.Ticker
}

func newNode(rf *Raft) *Node {
	return &Node{
		raft: rf,
		tick: time.NewTicker(tickTimeout * time.Millisecond),
	}
}

// 由于test里会不停get raft的内部状态，如state, currentTerm等，
// 因此需要对不同类别之间 进行加锁避免数据竞争
// 参考我的raft-lab的博客
func (n *Node) run() {
	for {
		select {
		case <-n.tick.C:
			n.raft.tick()
		case e := <-n.raft.eventCh:
			//TPrintf(n.raft.me, "Receive event %+v", e)
			if err := n.raft.Step(e); err != nil {
				DPrintf(n.raft.me, "Step event err %v", err)
			}
		case <-n.raft.killCh:
			DPrintf(n.raft.me, "Stop raft")
			return
		}
	}
}

func StartNode(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// 增加event channel buffer，保证send event尽量不会阻塞
	// 这也要求整体架构基于event处理，不使用volatile variables
	eventCh := make(chan Event, 100)
	rf := newRaft(peers, me, persister, applyCh, eventCh)
	rf.becomeFollower(rf.currentTerm, None)

	n := newNode(rf)

	go n.run()
	return rf
}
