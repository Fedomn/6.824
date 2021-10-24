package shardctrler

import (
	"fmt"
	"sort"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10
const InvalidGID = 0

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c Config) String() string {
	return fmt.Sprintf("{Num:%d,Shards:%v,Groups:%v}", c.Num, c.Shards, c.Groups)
}

func (c *Config) isInitial() bool {
	for _, gid := range c.Shards {
		if gid != InvalidGID {
			return false
		}
	}
	return true
}

func (c *Config) joinNewGroupsAndRebalanceShards(newGroups map[int][]string) {
	for gid, servers := range newGroups {
		if _, ok := c.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			c.Groups[gid] = newServers
		}
	}
	c.rebalanceShards()
}

func (c *Config) rebalanceShards() {
	if c.isInitial() {
		if gids := c.sortGIDsByGroups(); len(gids) > 0 {
			firstGID := gids[0]
			for idx := range c.Shards {
				c.Shards[idx] = firstGID
			}
		}
	}

	group2shards := c.group2shards()
	for {
		maxShardsGID, maxShardsCnt, minShardsGID, minShardsCnt := getMaxMinShardsGID(group2shards)
		if maxShardsCnt-minShardsCnt <= 1 {
			break
		}
		group2shards[minShardsGID] = append(group2shards[minShardsGID], group2shards[maxShardsGID][0])
		group2shards[maxShardsGID] = group2shards[maxShardsGID][1:]
	}
	var newShards [NShards]int
	for gid, shards := range group2shards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	c.Shards = newShards
}

func (c *Config) leaveGroupsAndReassignShards(gids []int) {
	orphanShards := make([]int, 0)
	group2shards := c.group2shards()
	for _, gid := range gids {
		if _, ok := c.Groups[gid]; ok {
			delete(c.Groups, gid)
		}
		if shards, ok := group2shards[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(group2shards, gid)
		}
	}
	c.Shards = reassignShards(orphanShards, group2shards)
}

func reassignShards(orphanShards []int, group2shards map[int][]int) [NShards]int {
	for _, shard := range orphanShards {
		_, _, minShardsGID, _ := getMaxMinShardsGID(group2shards)
		group2shards[minShardsGID] = append(group2shards[minShardsGID], shard)
	}

	var newShards [NShards]int
	for gid, shards := range group2shards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	return newShards
}

func getMaxMinShardsGID(group2shards map[int][]int) (maxShardsGID, maxShardsCnt, minShardsGID, minShardsCnt int) {
	maxShardsGID, maxShardsCnt = InvalidGID, -1
	minShardsGID, minShardsCnt = InvalidGID, NShards

	keys := make([]int, 0)
	for key := range group2shards {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	for _, key := range keys {
		val := group2shards[key]
		if len(val) > maxShardsCnt {
			maxShardsGID = key
			maxShardsCnt = len(val)
		}

		if len(val) < minShardsCnt {
			minShardsGID = key
			minShardsCnt = len(val)
		}
	}
	return
}

func (c *Config) sortGIDsByGroups() []int {
	keys := make([]int, 0)
	for key := range c.Groups {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

func (c *Config) group2shards() map[int][]int {
	group2shards := make(map[int][]int)
	if len(c.Groups) == 0 {
		return group2shards
	}

	// init group2shard map
	for gid := range c.Groups {
		if gid == 0 {
			continue
		}
		group2shards[gid] = make([]int, 0)
	}
	// calc group2shard count
	for shard, gid := range c.Shards {
		if gid == 0 {
			continue
		}
		group2shards[gid] = append(group2shards[gid], shard)
	}
	return group2shards
}
