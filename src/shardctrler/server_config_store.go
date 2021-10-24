package shardctrler

type MemoryConfigStore struct {
	Configs []Config
}

// Config #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
func NewMemoryConfigStore() *MemoryConfigStore {
	cf := &MemoryConfigStore{make([]Config, 1)}
	cf.Configs[0] = Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	return cf
}

// 新增 groups: gid -> servers
func (c *MemoryConfigStore) Join(newGroups map[int][]string) string {
	newConfig := c.newBaseConfig()
	newConfig.joinNewGroupsAndRebalanceShards(newGroups)
	c.Configs = append(c.Configs, *newConfig)
	return OK
}

func (c *MemoryConfigStore) Leave(gids []int) string {
	newConfig := c.newBaseConfig()
	newConfig.leaveGroupsAndReassignShards(gids)
	c.Configs = append(c.Configs, *newConfig)
	return OK
}

// 迁移shard到另外一个gid: replica group
func (c *MemoryConfigStore) Move(shard, gid int) string {
	newConfig := c.newBaseConfig()
	newConfig.Shards[shard] = gid
	c.Configs = append(c.Configs, *newConfig)
	return OK
}

func (c *MemoryConfigStore) Query(num int) (Config, string) {
	if num < 0 || num >= len(c.Configs) {
		return c.lastConfig(), OK
	}
	return c.Configs[num], OK
}

func (c *MemoryConfigStore) newBaseConfig() *Config {
	lastConfig := c.lastConfig()
	return &Config{len(c.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
}

func (c *MemoryConfigStore) lastConfig() Config {
	return c.Configs[len(c.Configs)-1]
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}
