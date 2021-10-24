package shardkv

import (
	"6.824/labgob"
	"bytes"
	"log"
)

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.lastApplied)
	_ = e.Encode(kv.kvStore)
	_ = e.Encode(kv.sessions)
	return w.Bytes()
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		kv.lastApplied = 0
		kv.kvStore = make(map[string]string)
		kv.sessions = make(map[int64]LastOperation)
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var kvStore map[string]string
	var sessions map[int64]LastOperation
	if d.Decode(&lastApplied) != nil || d.Decode(&kvStore) != nil || d.Decode(&sessions) != nil {
		log.Fatalf("ShardKVServerApplier decode:%v error", snapshot)
	}
	kv.lastApplied = lastApplied
	kv.kvStore = kvStore
	kv.sessions = sessions
}
