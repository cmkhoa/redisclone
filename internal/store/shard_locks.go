package store

import "sort"

// lockWriteShards acquires every shard needed for keys in increasing shard
// order. Ordering is the deadlock-prevention rule for future multi-key
// mutation: two clients may ask for keys in opposite orders, but they always
// take the underlying locks in the same order.
func (s *Store) lockWriteShards(keys []string) []uint8 {
	ids := make([]uint8, 0, len(keys))
	seen := make(map[uint8]struct{}, len(keys))
	for _, key := range keys {
		id := shardIndex(key)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		s.readShards[id].mu.Lock()
	}
	return ids
}

func (s *Store) unlockWriteShards(ids []uint8) {
	for i := len(ids) - 1; i >= 0; i-- {
		s.readShards[ids[i]].mu.Unlock()
	}
}
