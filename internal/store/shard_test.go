package store

import "testing"

func TestShardIndexIsStableAndDistributed(t *testing.T) {
	seen := make(map[uint8]bool)
	for i := 0; i < 4096; i++ {
		key := "key:" + itoa(i)
		first, second := shardIndex(key), shardIndex(key)
		if first != second {
			t.Fatalf("routing for %q changed: %d then %d", key, first, second)
		}
		if first >= shardCount {
			t.Fatalf("shard %d is outside [0,%d)", first, shardCount)
		}
		seen[first] = true
	}
	if len(seen) != shardCount {
		t.Errorf("only used %d of %d shards", len(seen), shardCount)
	}
}

func TestWriteShardLocksAreUniqueAndOrdered(t *testing.T) {
	s := New()
	keys := []string{"one", "two", "one", "three", "two"}
	ids := s.lockWriteShards(keys)
	defer s.unlockWriteShards(ids)
	for i := 1; i < len(ids); i++ {
		if ids[i-1] >= ids[i] {
			t.Fatalf("lock order %v is not strictly increasing", ids)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
