package app

import "sort"

type hashFunc = func(uint64) uint64

type Hash struct {
	replicas uint64
	hashF    hashFunc
	hashMap  map[uint64]uint64
	keys     []uint64
}

func NewHash(replicas uint64, fn hashFunc) *Hash {
	return &Hash{
		replicas: replicas,
		hashF:    fn,
		hashMap:  make(map[uint64]uint64),
		keys:     make([]uint64, 0),
	}
}

func (h *Hash) Add(servers []uint64) {
	for _, server := range servers {
		var replica uint64
		for replica = 0; replica < h.replicas; replica++ {
			key := h.hashF(replica + server)
			if _, ok := h.hashMap[key]; !ok {
				h.hashMap[key] = server
				h.keys = append(h.keys, key)
			}
		}
	}
	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

func (h *Hash) Delete(servers []uint64) {
	for _, server := range servers {
		var replica uint64
		for replica = 0; replica < h.replicas; replica++ {
			key := h.hashF(replica + server)
			if val, ok := h.hashMap[key]; ok && val == server {
				delete(h.hashMap, key)
			}
		}
	}
	h.keys = make([]uint64, 0)
	for key, _ := range h.hashMap {
		h.keys = append(h.keys, key)
	}
	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

func (h *Hash) Hash(key uint64) (server uint64) {
	if len(h.keys) == 0 {
		return 0
	}
	hash := h.hashF(key)
	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})
	if idx == len(h.keys) {
		idx = 0
	}
	return h.hashMap[h.keys[idx]]
}
