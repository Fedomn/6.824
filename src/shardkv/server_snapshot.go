package shardkv

import (
	"6.824/labgob"
	"6.824/shardctrler"
	"bytes"
	"log"
)

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.lastApplied)
	_ = e.Encode(kv.shardStore)
	_ = e.Encode(kv.sessions)
	_ = e.Encode(kv.lastConfig)
	_ = e.Encode(kv.currentConfig)
	return w.Bytes()
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		kv.lastApplied = 0
		kv.shardStore = make(map[int]Shard)
		for i := 0; i < shardctrler.NShards; i++ {
			kv.shardStore[i] = Shard{
				KV:     make(map[string]string),
				Status: ShardServing,
			}
		}
		kv.sessions = make(map[int64]LastOperation)
		kv.lastConfig = shardctrler.DefaultConfig()
		kv.currentConfig = shardctrler.DefaultConfig()
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var store map[int]Shard
	var sessions map[int64]LastOperation
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config
	if d.Decode(&lastApplied) != nil || d.Decode(&store) != nil || d.Decode(&sessions) != nil || d.Decode(&lastConfig) != nil || d.Decode(&currentConfig) != nil {
		log.Fatalf("ShardKVServerApplier decode:%v error", snapshot)
	}
	kv.lastApplied = lastApplied
	kv.shardStore = store
	kv.sessions = sessions
	kv.lastConfig = lastConfig
	kv.currentConfig = currentConfig
}
