package shardkv

import (
	"6.824/shardctrler"
	"sync"
	"time"
)

func (kv *ShardKV) monitorConfiguration() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			canPullConfig := true
			for shardId, shard := range kv.shardStore {
				if shard.Status != ShardServing {
					canPullConfig = false
					DPrintf(kv.gid, kv.me, "ShardCtrlerMonitorConfiguration CanNotPullConfig, because shard%s status %s", shardId, shard.Status)
					break
				}
			}
			currentConfigNum := kv.currentConfig.Num
			kv.mu.RUnlock()

			if canPullConfig {
				latestConfig := kv.sc.Query(currentConfigNum + 1)
				if latestConfig.Num == currentConfigNum+1 {
					DPrintf(kv.gid, kv.me, "ShardCtrlerMonitorConfiguration PulledLatestConfig %v", latestConfig)
					kv.StartCmdAndWait(Command{CmdConfig, latestConfig}, &CmdReply{})
				}
			}

		}
		time.Sleep(MonitorConfigTimeout)
	}
}

func (kv *ShardKV) monitorPull() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			var wg sync.WaitGroup
			gid2shardIDs := kv.getShardIDsByStatus(ShardPulling)
			currentConfigNum := kv.currentConfig.Num
			lastConfig := kv.lastConfig
			kv.mu.RUnlock()

			for gid, shardIDs := range gid2shardIDs {
				DPrintf(kv.gid, kv.me, "ShardCtrlerMonitorPull StartPullShards from gid:%d, shardIDs:%v", gid, shardIDs)
				_gid := gid
				_shardIDs := shardIDs
				wg.Add(1)
				go func() {
					kv.pullShardData(lastConfig, currentConfigNum, _gid, _shardIDs)
					wg.Done()
				}()
			}
			wg.Wait()
		}
		time.Sleep(MonitorPullTimeout)
	}
}

func (kv *ShardKV) getShardIDsByStatus(status ShardStatus) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	for shardId, shard := range kv.shardStore {
		if shard.Status == status {
			if gid := kv.lastConfig.Shards[shardId]; gid != 0 {
				if _, ok := gid2shardIDs[gid]; !ok {
					gid2shardIDs[gid] = make([]int, 0)
				}
				gid2shardIDs[gid] = append(gid2shardIDs[gid], shardId)
			}
		}
	}
	return gid2shardIDs
}

func (kv *ShardKV) pullShardData(lastConfig shardctrler.Config, configNum, gid int, shardIDs []int) {
	pullArgs := ShardsOpArgs{configNum, shardIDs}
	servers := lastConfig.Groups[gid]
	for _, server := range servers {
		pullReply := ShardsOpReply{}
		srv := kv.make_end(server)
		if srv.Call("ShardKV.GetShardsData", &pullArgs, &pullReply) && pullReply.Status == OK {
			DPrintf(kv.gid, kv.me, "ShardCtrlerMonitorPull PullShardData success, will startInsertShardsCommand")
			kv.StartCmdAndWait(Command{CmdInsertShards, pullReply}, &CmdReply{})
			return
		}
	}
}

func (kv *ShardKV) monitorGC() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			var wg sync.WaitGroup
			gid2shardIDs := kv.getShardIDsByStatus(ShardGCing)
			currentConfigNum := kv.currentConfig.Num
			lastConfig := kv.lastConfig
			kv.mu.RUnlock()

			for gid, shardIDs := range gid2shardIDs {
				DPrintf(kv.gid, kv.me, "ShardCtrlerMonitorGC StartDeleteShards from gid:%d, shardIDs:%v", gid, shardIDs)
				_gid := gid
				_shardIDs := shardIDs
				wg.Add(1)
				go func() {
					kv.deleteShardData(lastConfig, currentConfigNum, _gid, _shardIDs)
					wg.Done()
				}()
			}
			wg.Wait()
		}
		time.Sleep(MonitorGCTimeout)
	}
}

func (kv *ShardKV) deleteShardData(lastConfig shardctrler.Config, configNum, gid int, shardIDs []int) {
	pullArgs := ShardsOpArgs{configNum, shardIDs}
	servers := lastConfig.Groups[gid]
	for _, server := range servers {
		pullReply := ShardsOpReply{}
		srv := kv.make_end(server)
		if srv.Call("ShardKV.DeleteShardsData", &pullArgs, &pullReply) && pullReply.Status == OK {
			DPrintf(kv.gid, kv.me, "ShardCtrlerMonitorGC DeleteShardData success, will startDeleteShardsCommand")
			// 这里的CmdDeleteShards目的：重置 仅为标记的ShardGCing shard 为 ShardServing
			kv.StartCmdAndWait(Command{CmdDeleteShards, pullArgs}, &CmdReply{})
			return
		}
	}
}
