package store

import (
	"fmt"
	"testing"
	"time"
)

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

// This exercises the first production user of lockWriteShards. It matters
// even while Store.mu still serializes mutations: the transition is where a
// later removal of that lock could otherwise turn a harmless implementation
// detail into an opposite-order deadlock.
func TestMultiKeyMutationsKeepShardStateConsistent(t *testing.T) {
	s := New()
	keys := keysOnDistinctShards(t, 3)

	// The final occurrence of a duplicate MSET key wins, and all of the keys
	// are deliberately on different shards.
	s.MSet([]Pair{
		{Key: keys[2], Value: []byte("three")},
		{Key: keys[0], Value: []byte("old")},
		{Key: keys[1], Value: []byte("two")},
		{Key: keys[0], Value: []byte("one")},
	})
	for key, want := range map[string]string{
		keys[0]: "one",
		keys[1]: "two",
		keys[2]: "three",
	} {
		got, ok := s.Get(key)
		if !ok || string(got) != want {
			t.Errorf("Get(%q) = %q, %v; want %q, true", key, got, ok, want)
		}
	}
	checkShardMirror(t, s)

	// Reverse order is the adversarial order a concurrent caller may use.
	// A duplicate in DEL is still counted once and all mirror state is removed.
	if got := s.Del(keys[2], keys[1], keys[0], keys[1]); got != 3 {
		t.Fatalf("Del across shards = %d, want 3", got)
	}
	checkShardMirror(t, s)
	if got := s.Len(); got != 0 {
		t.Errorf("Len after cross-shard Del = %d, want 0", got)
	}
}

// Single-key writes may coordinate through the durable gate, but their data
// mutation must touch exactly the key's shard.
func TestSingleKeyMutationsTouchOnlyOwningShard(t *testing.T) {
	s := New()
	keys := keysOnDistinctShards(t, 2)
	key, other := keys[0], keys[1]
	s.Set(other, []byte("other"))
	otherID := shardIndex(other)

	assertOtherUnchanged := func(op string) {
		t.Helper()
		shard := &s.readShards[otherID]
		shard.mu.RLock()
		defer shard.mu.RUnlock()
		e := shard.data[other]
		if e == nil || string(e.val) != "other" || !e.expiresAt.IsZero() {
			t.Errorf("%s changed unrelated shard entry: %#v", op, e)
		}
		if len(shard.data) != 1 || len(shard.expiring) != 0 {
			t.Errorf("%s changed unrelated shard state: %d keys, %d expiring", op, len(shard.data), len(shard.expiring))
		}
	}

	s.SetWithTTL(key, []byte("1"), time.Hour)
	assertOtherUnchanged("SET with TTL")
	if _, err := s.Increment(key, 1); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	assertOtherUnchanged("INCR")
	s.Append(key, []byte("x"))
	assertOtherUnchanged("APPEND")
	if !s.Expire(key, time.Hour) {
		t.Fatal("EXPIRE reported missing key")
	}
	assertOtherUnchanged("EXPIRE")
	if got := s.Del(key); got != 1 {
		t.Fatalf("DEL = %d, want 1", got)
	}
	assertOtherUnchanged("DEL")
	checkShardMirror(t, s)
}

func keysOnDistinctShards(t *testing.T, n int) []string {
	t.Helper()
	keys := make([]string, 0, n)
	seen := make(map[uint8]bool, n)
	for i := 0; len(keys) < n; i++ {
		key := fmt.Sprintf("shard-key-%d", i)
		id := shardIndex(key)
		if seen[id] {
			continue
		}
		seen[id] = true
		keys = append(keys, key)
	}
	return keys
}

// checkShardMirror validates shard-local key, expiry-index, and memory
// invariants after a mutation.
func checkShardMirror(t *testing.T, s *Store) {
	t.Helper()
	var shardUsed int64
	for i := range s.readShards {
		shard := &s.readShards[i]
		shard.mu.RLock()
		for key, e := range shard.data {
			_, indexed := shard.expiring[key]
			if indexed != !e.expiresAt.IsZero() {
				t.Errorf("shard expiry index for %q = %v, want %v", key, indexed, !e.expiresAt.IsZero())
			}
		}
		for key := range shard.expiring {
			if _, ok := shard.data[key]; !ok {
				t.Errorf("shard expiry index contains absent key %q", key)
			}
		}
		shardUsed += shard.used
		shard.mu.RUnlock()
	}
	if shardUsed != s.Used() {
		t.Errorf("shard used total = %d, Used = %d", shardUsed, s.Used())
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
