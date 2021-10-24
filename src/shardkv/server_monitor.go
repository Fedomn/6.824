package shardkv

import "time"

func (kv *ShardKV) monitorConfiguration() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			canPullConfig := true
			for _, shard := range kv.shardStore {
				if shard.Status != ShardServing {
					canPullConfig = false
					DPrintf(kv.gid, kv.me, "Can not pull config, because ShardStatus %s", shard.Status)
					break
				}
			}
			currentConfigNum := kv.currentConfig.Num
			kv.mu.RUnlock()

			if canPullConfig {
				latestConfig := kv.sc.Query(currentConfigNum + 1)
				if latestConfig.Num == currentConfigNum+1 {
					DPrintf(kv.gid, kv.me, "Pulled latestConfig %v", latestConfig)
					kv.StartCmdAndWait(Command{CmdConfig, latestConfig}, &CmdReply{})
				}
			}

		}
		time.Sleep(MonitorConfigTimeout)
	}
}
