package v2

import (
	"6.824/labrpc"
	"time"
)

type Node struct {
	raft    *Raft
	eventCh chan Event
	tick    *time.Ticker
}

func newNode(rf *Raft, eventCh chan Event) *Node {
	return &Node{
		raft:    rf,
		eventCh: eventCh,
		tick:    time.NewTicker(tickTimeout * time.Millisecond),
	}
}

func (n *Node) run() {
	for {
		select {
		case <-n.tick.C:
			n.raft.tick()
		case e := <-n.eventCh:
			TPrintf(n.raft.me, "Receive event %+v", e)
			if err := n.raft.Step(e); err != nil {
				DPrintf(n.raft.me, "Step event err %v", err)
			}
		}
	}
}

func StartNode(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	eventCh := make(chan Event, 1)
	rf := newRaft(peers, me, persister, applyCh, eventCh)
	rf.becomeFollower(0, None)

	n := newNode(rf, eventCh)

	go n.run()
	return rf
}
