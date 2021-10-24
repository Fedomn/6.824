package shardctrler

import (
	"reflect"
	"testing"
)

func TestConfig_group2shards(t *testing.T) {
	type fields struct {
		Num    int
		Shards [NShards]int
		Groups map[int][]string
	}
	tests := []struct {
		name   string
		fields fields
		want   map[int][]int
	}{
		{
			name: "normal case",
			fields: fields{
				Num:    0,
				Shards: [NShards]int{2, 2, 2, 2, 2, 1, 1, 1, 1, 1},
				Groups: map[int][]string{2: {"x"}, 1: {"x"}},
			},
			want: map[int][]int{2: {0, 1, 2, 3, 4}, 1: {5, 6, 7, 8, 9}},
		},
		{
			name: "empty case",
			fields: fields{
				Num:    0,
				Shards: [NShards]int{},
				Groups: map[int][]string{},
			},
			want: map[int][]int{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Num:    tt.fields.Num,
				Shards: tt.fields.Shards,
				Groups: tt.fields.Groups,
			}
			if got := c.group2shards(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("group2shards() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getMaxMinShardsGID(t *testing.T) {
	type args struct {
		group2shards map[int][]int
	}
	tests := []struct {
		name             string
		args             args
		wantMaxShardsGID int
		wantMaxShardsCnt int
		wantMinShardsGID int
		wantMinShardsCnt int
	}{
		{
			name: "normal case",
			args: args{
				group2shards: map[int][]int{
					1: {0, 1, 2, 3},
					2: {4},
					3: {5, 6, 7, 8, 9},
				},
			},
			wantMaxShardsGID: 3,
			wantMaxShardsCnt: 5,
			wantMinShardsGID: 2,
			wantMinShardsCnt: 1,
		},
		{
			name: "same case",
			args: args{
				group2shards: map[int][]int{
					1: {0, 1, 2},
					2: {3, 4, 5},
					3: {6, 7},
					4: {8, 9},
				},
			},
			wantMaxShardsGID: 1,
			wantMaxShardsCnt: 3,
			wantMinShardsGID: 3,
			wantMinShardsCnt: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMaxShardsGID, gotMaxShardsCnt, gotMinShardsGID, gotMinShardsCnt := getMaxMinShardsGID(tt.args.group2shards)
			if gotMaxShardsGID != tt.wantMaxShardsGID {
				t.Errorf("getMaxMinShardsGID() gotMaxShardsGID = %v, want %v", gotMaxShardsGID, tt.wantMaxShardsGID)
			}
			if gotMaxShardsCnt != tt.wantMaxShardsCnt {
				t.Errorf("getMaxMinShardsGID() gotMaxShardsCnt = %v, want %v", gotMaxShardsCnt, tt.wantMaxShardsCnt)
			}
			if gotMinShardsGID != tt.wantMinShardsGID {
				t.Errorf("getMaxMinShardsGID() gotMinShardsGID = %v, want %v", gotMinShardsGID, tt.wantMinShardsGID)
			}
			if gotMinShardsCnt != tt.wantMinShardsCnt {
				t.Errorf("getMaxMinShardsGID() gotMinShardsCnt = %v, want %v", gotMinShardsCnt, tt.wantMinShardsCnt)
			}
		})
	}
}

func Test_reassignShards(t *testing.T) {
	type args struct {
		orphanShards []int
		group2shards map[int][]int
	}
	tests := []struct {
		name string
		args args
		want [NShards]int
	}{
		{
			name: "normal case",
			args: args{
				orphanShards: []int{0, 1, 2},
				group2shards: map[int][]int{
					1: {3, 4, 5, 6, 7},
					2: {8},
					3: {9},
				},
			},
			want: [NShards]int{2, 3, 2, 1, 1, 1, 1, 1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reassignShards(tt.args.orphanShards, tt.args.group2shards); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("reassignShards() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_leaveGroupsAndReassignShards(t *testing.T) {
	type fields struct {
		Num    int
		Shards [NShards]int
		Groups map[int][]string
	}
	type args struct {
		gids []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   [NShards]int
	}{
		{
			name: "normal case",
			fields: fields{
				Num:    0,
				Shards: [NShards]int{1, 1, 1, 1, 2, 2, 2, 3, 3, 3},
				Groups: map[int][]string{
					1: {"x"},
					2: {"x"},
					3: {"x"},
				},
			},
			args: args{gids: []int{1}},
			want: [NShards]int{2, 3, 2, 3, 2, 2, 2, 3, 3, 3},
		},
		{
			name: "leave all groups case",
			fields: fields{
				Num:    0,
				Shards: [NShards]int{1, 1, 1, 1, 2, 2, 2, 3, 3, 3},
				Groups: map[int][]string{
					1: {"x"},
					2: {"x"},
					3: {"x"},
				},
			},
			args: args{gids: []int{1, 2, 3}},
			want: [NShards]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Num:    tt.fields.Num,
				Shards: tt.fields.Shards,
				Groups: tt.fields.Groups,
			}
			c.leaveGroupsAndReassignShards(tt.args.gids)
			if got := c.Shards; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("leaveGroupsAndReassignShards() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_rebalanceShards(t *testing.T) {
	type fields struct {
		Num    int
		Shards [NShards]int
		Groups map[int][]string
	}
	tests := []struct {
		name   string
		fields fields
		want   [NShards]int
	}{
		{
			name: "normal case",
			fields: fields{
				Num:    0,
				Shards: [NShards]int{1, 1, 2, 2, 2, 2, 2, 2, 2, 3},
				Groups: map[int][]string{
					1: {"x", "y"},
					2: {"x", "y"},
					3: {"x", "y"},
				},
			},
			want: [NShards]int{1, 1, 3, 1, 3, 2, 2, 2, 2, 3},
		},
		{
			name: "join and rebalance 1",
			fields: fields{
				Num:    0,
				Shards: [NShards]int{},
				Groups: map[int][]string{1: {"x"}},
			},
			want: [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			name: "join and rebalance 2",
			fields: fields{
				Num:    0,
				Shards: [NShards]int{},
				Groups: map[int][]string{1: {"x"}, 2: {"x"}},
			},
			want: [NShards]int{2, 2, 2, 2, 2, 1, 1, 1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Num:    tt.fields.Num,
				Shards: tt.fields.Shards,
				Groups: tt.fields.Groups,
			}
			c.rebalanceShards()
			if got := c.Shards; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("rebalanceShards() = %v, want %v", got, tt.want)
			}
		})
	}
}
