package shardmaster

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	pb "github.com/singlemonad/SnailKV/proto"

	"github.com/singlemonad/SnailKV/shardmaster"
)

// import "time"

func checkZones(z1, z2 map[uint64]uint64) bool {
	if len(z1) != len(z2) {
		return false
	}

	ks := make([]uint64, 0)
	for k, _ := range z1 {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		return ks[i] < ks[j]
	})

	for _, k := range ks {
		if _, ok := z2[k]; !ok {
			return false
		}
		if z1[k] != z2[k] {
		}
		return false
	}
	return true
}

func check(t *testing.T, groups []int, ck *shardmaster.Clerk) {
	c := ck.Query(-1)
	if len(c.Groups) != len(groups) {
		t.Fatalf("wanted %v groups, got %v", len(groups), len(c.Groups))
	}

	// are the groups as expected?
	for _, g := range groups {
		_, ok := c.Groups[uint64(g)]
		if ok != true {
			t.Fatalf("missing group %v", g)
		}
	}

	// any un-allocated shards?
	if len(groups) > 0 {
		for s, g := range c.Zones {
			_, ok := c.Groups[g]
			if ok == false {
				t.Fatalf("shard %v -> invalid group %v", s, g)
			}
		}
	}

	// more or less balanced sharding?
	counts := map[int]int{}
	for _, g := range c.Zones {
		counts[int(g)] += 1
	}
	min := 257
	max := 0
	for g, _ := range c.Groups {
		if counts[int(g)] > max {
			max = counts[int(g)]
		}
		if counts[int(g)] < min {
			min = counts[int(g)]
		}
	}
	if max > min+1 {
		t.Fatalf("max %v too much larger than min %v", max, min)
	}
}

func check_same_config(t *testing.T, c1 *pb.ClusterConfig, c2 *pb.ClusterConfig) {
	if c1.Version != c2.Version {
		t.Fatalf("Version wrong")
	}
	if checkZones(c1.Zones, c2.Zones) {
		t.Fatalf("Shards wrong")
	}
	if len(c1.Groups) != len(c2.Groups) {
		t.Fatalf("number of Groups is wrong")
	}
	for gid, sa := range c1.Groups {
		sa1, ok := c2.Groups[gid]
		if ok == false || len(sa1.Strings) != len(sa.Strings) {
			t.Fatalf("len(Groups) wrong")
		}
		if ok && len(sa1.Strings) == len(sa.Strings) {
			for j := 0; j < len(sa.Strings); j++ {
				if sa.Strings[j] != sa1.Strings[j] {
					t.Fatalf("Groups wrong")
				}
			}
		}
	}
}

func TestBasic(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: Basic leave/join ...\n")

	cfa := make([]*pb.ClusterConfig, 6)
	cfa[0] = ck.Query(-1)

	check(t, []int{}, ck)

	var gid1 uint64 = 1
	ck.Join(map[uint64][]string{uint64(gid1): []string{"x", "y", "z"}})
	check(t, []int{int(gid1)}, ck)
	cfa[1] = ck.Query(-1)

	var gid2 uint64 = 2
	ck.Join(map[uint64][]string{uint64(gid2): []string{"a", "b", "c"}})
	check(t, []int{int(gid1), int(gid2)}, ck)
	cfa[2] = ck.Query(-1)

	ck.Join(map[uint64][]string{uint64(gid2): []string{"a", "b", "c"}})
	check(t, []int{int(gid1), int(gid2)}, ck)
	cfa[3] = ck.Query(-1)

	cfx := ck.Query(-1)
	sa1 := cfx.Groups[uint64(gid1)]
	if len(sa1.Strings) != 3 || sa1.Strings[0] != "x" || sa1.Strings[1] != "y" || sa1.Strings[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
	}
	sa2 := cfx.Groups[gid2]
	if len(sa2.Strings) != 3 || sa2.Strings[0] != "a" || sa2.Strings[1] != "b" || sa2.Strings[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}

	fmt.Printf("111111\n")

	ck.Leave([]uint64{gid1})
	check(t, []int{int(gid2)}, ck)
	cfa[4] = ck.Query(-1)
	fmt.Printf("222222\n")

	ck.Leave([]uint64{gid1})
	check(t, []int{int(gid2)}, ck)
	cfa[5] = ck.Query(-1)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Historical queries ...\n")

	for s := 0; s < nservers; s++ {
		cfg.ShutdownServer(s)
		for i := 0; i < len(cfa); i++ {
			c := ck.Query(cfa[i].Version)
			//fmt.Printf("c = %v, cfa[%d] = %v\n", c, i, cfa[i])
			check_same_config(t, c, cfa[i])
		}
		cfg.StartServer(s)
		cfg.ConnectAll()
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Move ...\n")
	{
		var gid3 uint64 = 503
		ck.Join(map[uint64][]string{gid3: []string{"3a", "3b", "3c"}})
		var gid4 uint64 = 504
		ck.Join(map[uint64][]string{gid4: []string{"4a", "4b", "4c"}})
		NShards := uint64(len(ck.Query(0).Zones))
		for i := uint64(0); i < NShards; i++ {
			cf := ck.Query(-1)
			if i < NShards/2 {
				ck.Move(i, gid3)
				if cf.Zones[i] != gid3 {
					cf1 := ck.Query(-1)
					if cf1.Version <= cf.Version {
						t.Fatalf("Move should increase Config.Num")
					}
				}
			} else {
				ck.Move(i, gid4)
				if cf.Zones[i] != gid4 {
					cf1 := ck.Query(-1)
					if cf1.Version <= cf.Version {
						t.Fatalf("Move should increase Config.Num")
					}
				}
			}
		}
		cf2 := ck.Query(-1)
		for i := uint64(0); i < NShards; i++ {
			if i < NShards/2 {
				if cf2.Zones[i] != gid3 {
					t.Fatalf("expected shard %v on gid %v actually %v",
						i, gid3, cf2.Zones[i])
				}
			} else {
				if cf2.Zones[i] != gid4 {
					t.Fatalf("expected shard %v on gid %v actually %v",
						i, gid4, cf2.Zones[i])
				}
			}
		}
		ck.Leave([]uint64{gid3})
		ck.Leave([]uint64{gid4})
	}
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent leave/join ...\n")

	const npara = 10
	var cka [npara]*shardmaster.Clerk
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeClient(cfg.All())
	}
	gids := make([]int, npara)
	ch := make(chan bool)
	for xi := 0; xi < npara; xi++ {
		gids[xi] = int(xi + 1)
		go func(i int) {
			defer func() { ch <- true }()
			var gid uint64 = uint64(gids[i])
			cka[i].Join(map[uint64][]string{gid + 1000: []string{"a", "b", "c"}})
			cka[i].Join(map[uint64][]string{gid: []string{"a", "b", "c"}})
			cka[i].Leave([]uint64{gid + 1000})
		}(xi)
	}
	for i := 0; i < npara; i++ {
		<-ch
	}
	check(t, gids, ck)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after joins ...\n")

	c1 := ck.Query(-1)
	for i := 0; i < 5; i++ {
		ck.Join(map[uint64][]string{uint64(npara + 1 + i): []string{"a", "b", "c"}})
	}
	c2 := ck.Query(-1)
	for i := uint64(1); i <= npara; i++ {
		for j := uint64(0); j < uint64(len(c1.Zones)); j++ {
			if c2.Zones[j] == i {
				if c1.Zones[j] != i {
					t.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after leaves ...\n")

	for i := 0; i < 5; i++ {
		ck.Leave([]uint64{uint64(npara + 1 + i)})
	}
	c3 := ck.Query(-1)
	for i := uint64(1); i <= npara; i++ {
		for j := uint64(0); j < uint64(len(c1.Zones)); j++ {
			if c2.Zones[j] == i {
				if c3.Zones[j] != i {
					t.Fatalf("non-minimal transfer after Leave()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestMulti(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: Multi-group join/leave ...\n")

	cfa := make([]*pb.ClusterConfig, 6)
	cfa[0] = ck.Query(-1)

	check(t, []int{}, ck)

	var gid1 uint64 = 1
	var gid2 uint64 = 2
	ck.Join(map[uint64][]string{
		gid1: []string{"x", "y", "z"},
		gid2: []string{"a", "b", "c"},
	})
	check(t, []int{int(gid1), int(gid2)}, ck)
	fmt.Printf("111111\n")
	cfa[1] = ck.Query(-1)

	var gid3 uint64 = 3
	ck.Join(map[uint64][]string{gid3: []string{"j", "k", "l"}})
	check(t, []int{int(gid1), int(gid2), int(gid3)}, ck)
	fmt.Printf("222222\n")
	cfa[2] = ck.Query(-1)

	ck.Join(map[uint64][]string{gid2: []string{"a", "b", "c"}})
	check(t, []int{int(gid1), int(gid2), int(gid3)}, ck)
	fmt.Printf("333333\n")
	cfa[3] = ck.Query(-1)

	cfx := ck.Query(-1)
	sa1 := cfx.Groups[gid1]
	if len(sa1.Strings) != 3 || sa1.Strings[0] != "x" || sa1.Strings[1] != "y" || sa1.Strings[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
	}
	fmt.Printf("444444\n")
	sa2 := cfx.Groups[gid2]
	if len(sa2.Strings) != 3 || sa2.Strings[0] != "a" || sa2.Strings[1] != "b" || sa2.Strings[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}
	fmt.Printf("555555\n")
	sa3 := cfx.Groups[gid3]
	if len(sa3.Strings) != 3 || sa3.Strings[0] != "j" || sa3.Strings[1] != "k" || sa3.Strings[2] != "l" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid3, sa3)
	}
	fmt.Printf("666666\n")

	ck.Leave([]uint64{gid1, gid3})
	check(t, []int{int(gid2)}, ck)
	fmt.Printf("777777\n")
	cfa[4] = ck.Query(-1)

	cfx = ck.Query(-1)
	sa2 = cfx.Groups[gid2]
	if len(sa2.Strings) != 3 || sa2.Strings[0] != "a" || sa2.Strings[1] != "b" || sa2.Strings[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent multi leave/join ...\n")

	const npara = 10
	var cka [npara]*shardmaster.Clerk
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeClient(cfg.All())
	}
	gids := make([]int, npara)
	var wg sync.WaitGroup
	for xi := 0; xi < npara; xi++ {
		wg.Add(1)
		gids[xi] = int(xi + 1)
		go func(i int) {
			defer wg.Done()
			var gid uint64 = uint64(gids[i])
			cka[i].Join(map[uint64][]string{
				gid:        []string{"a", "b", "c"},
				gid + 1000: []string{"a", "b", "c"},
				gid + 2000: []string{"a", "b", "c"},
			})
			cka[i].Leave([]uint64{gid + 1000, gid + 2000})
		}(xi)
	}
	wg.Wait()
	check(t, gids, ck)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after multijoins ...\n")

	c1 := ck.Query(-1)
	m := make(map[uint64][]string)
	for i := 0; i < 5; i++ {
		m[uint64(npara+1+i)] = []string{"a", "b", "c"}
	}
	ck.Join(m)
	c2 := ck.Query(-1)
	for i := uint64(1); i <= npara; i++ {
		for j := uint64(0); j < uint64(len(c1.Zones)); j++ {
			if c2.Zones[j] == i {
				if c1.Zones[j] != i {
					t.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after multileaves ...\n")

	var l []uint64
	for i := 0; i < 5; i++ {
		l = append(l, uint64(npara+1+i))
	}
	ck.Leave(l)
	c3 := ck.Query(-1)
	for i := uint64(1); i <= npara; i++ {
		for j := uint64(0); j < uint64(len(c1.Zones)); j++ {
			if c2.Zones[j] == i {
				if c3.Zones[j] != i {
					t.Fatalf("non-minimal transfer after Leave()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}
