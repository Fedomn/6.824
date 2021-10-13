package raft

import (
	"6.824/labgob"
	"bytes"
)

func (rf *Raft) persist() {
	data := rf.encodeRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.sendRpcLatestSeq) != nil ||
		e.Encode(rf.recvRpcLatestSeq) != nil {
		panic("persist encounter error")
	}
	data := w.Bytes()
	return data
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var x int
	var y int
	var z []LogEntry
	var a map[int]*RpcSeqStatus
	var b map[int]uint32
	if d.Decode(&x) != nil ||
		d.Decode(&y) != nil ||
		d.Decode(&z) != nil ||
		d.Decode(&a) != nil ||
		d.Decode(&b) != nil {
		panic("readPersist encounter error")
	} else {
		rf.currentTerm = x
		rf.votedFor = y
		rf.log = z
		rf.sendRpcLatestSeq = a
		rf.recvRpcLatestSeq = b
	}
}
