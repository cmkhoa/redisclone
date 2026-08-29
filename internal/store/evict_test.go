package store

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func TestParseMemory(t *testing.T) {
	ok := []struct {
		in   string
		want int64
	}{
		{"0", 0}, {"1024", 1024}, {"1kb", 1 << 10}, {"100mb", 100 << 20},
		{"2gb", 2 << 30}, {"5M", 5 << 20}, {" 64mb ", 64 << 20}, {"512b", 512},
	}
	for _, tt := range ok {
		got, err := ParseMemory(tt.in)
		if err != nil {
			t.Errorf("ParseMemory(%q): %v", tt.in, err)
		} else if got != tt.want {
			t.Errorf("ParseMemory(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
	for _, bad := range []string{"", "lots", "-1", "1tb", "mb", "1.5mb"} {
		if _, err := ParseMemory(bad); err == nil {
			t.Errorf("ParseMemory(%q) was accepted", bad)
		}
	}
}

func TestMemoryAccounting(t *testing.T) {
	s := New()
	if s.Used() != 0 {
		t.Fatalf("a new store reports %d bytes used", s.Used())
	}

	s.Set("key", []byte("value"))
	afterOne := s.Used()
	if afterOne != entrySize("key", []byte("value")) {
		t.Errorf("Used = %d, want %d", afterOne, entrySize("key", []byte("value")))
	}

	// An overwrite adjusts rather than accumulates: this is the leak that would
	// otherwise take hours to find, because the keyspace looks right and only
	// the accounting drifts.
	s.Set("key", []byte("a much longer value"))
	if got, want := s.Used(), entrySize("key", []byte("a much longer value")); got != want {
		t.Errorf("after overwrite Used = %d, want %d", got, want)
	}

	s.Set("other", []byte("v"))
	s.Del("key", "other")
	if s.Used() != 0 {
		t.Errorf("Used = %d after deleting everything, want 0", s.Used())
	}

	// Expiry has to give the memory back too, by both routes.
	s.SetWithTTL("volatile", []byte("v"), time.Hour)
	plantExpired(s, "dead")
	s.Get("dead")         // lazy collection
	s.activeExpireCycle() // and the sampler
	if got, want := s.Used(), entrySize("volatile", []byte("v")); got != want {
		t.Errorf("Used = %d after collecting an expired key, want %d", got, want)
	}
}

// The per-entry overhead constant is a guess unless it is checked against
// reality. This measures actual heap growth for 200k keys and asserts the
// estimate is in the right neighbourhood.
//
// Not an exact assertion, and it should not be: Go's allocator rounds to size
// classes, maps grow in power-of-two steps and keep the old buckets alive
// briefly, and the GC has its own opinions about when memory comes back.
// Anything within a factor of ~1.5 means the constant is honest.
func TestMemoryEstimateTracksRealHeapGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a few hundred MB")
	}
	const n = 200_000

	s := New()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	for i := 0; i < n; i++ {
		s.Set(fmt.Sprintf("key:%08d", i), []byte("a fairly typical cached value"))
	}

	runtime.GC()
	runtime.ReadMemStats(&after)
	real := int64(after.HeapAlloc - before.HeapAlloc)
	estimate := s.Used()

	ratio := float64(estimate) / float64(real)
	t.Logf("estimate %d bytes, real heap growth %d bytes (ratio %.2f)", estimate, real, ratio)
	if ratio < 0.66 || ratio > 1.5 {
		t.Errorf("estimate is off by more than 1.5x (ratio %.2f); entryOverhead=%d needs revisiting",
			ratio, entryOverhead)
	}
	runtime.KeepAlive(s)
}

// setAtime plants a deterministic access time. The real clock has 100ms
// resolution, which is the right trade for the hot path and useless for a test
// that wants to know exactly which key is oldest.
func setAtime(s *Store, key string, at uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.data[key].atime.Store(at)
}

func TestNoEvictionRefusesInsteadOfDropping(t *testing.T) {
	s := New()
	s.Set("a", []byte("1"))
	s.Set("b", []byte("2"))
	s.SetMemoryLimit(entrySize("a", []byte("1")), PolicyNoEviction)

	evicted, ok := s.EnforceMemoryLimit()
	if ok {
		t.Error("EnforceMemoryLimit reported success while over the limit")
	}
	if evicted != 0 {
		t.Errorf("evicted %d keys under noeviction", evicted)
	}
	if n := s.Len(); n != 2 {
		t.Errorf("keyspace lost keys under noeviction: %d left of 2", n)
	}
}

func TestLRUEvictsTheOldestOfItsSample(t *testing.T) {
	s := New()
	// A sample of 5 out of 5 keys is the whole keyspace, which makes this
	// deterministic: the oldest key must be the one that goes.
	for i, k := range []string{"oldest", "b", "c", "d", "e"} {
		s.Set(k, []byte("v"))
		setAtime(s, k, uint64(100+i*10))
	}
	setAtime(s, "oldest", 1)

	// Room for four of the five.
	s.SetMemoryLimit(4*entrySize("b", []byte("v")), PolicyAllKeysLRU)

	evicted, ok := s.EnforceMemoryLimit()
	if !ok {
		t.Fatal("EnforceMemoryLimit could not get under the limit")
	}
	if evicted != 1 {
		t.Errorf("evicted %d keys, want 1", evicted)
	}
	if _, found := s.Get("oldest"); found {
		t.Error("the least recently used key survived")
	}
	if n := s.Len(); n != 4 {
		t.Errorf("%d keys left, want 4", n)
	}
}

// Reading a key must protect it: that is the whole difference between LRU and
// "evict whatever".
func TestReadingAKeyProtectsItFromEviction(t *testing.T) {
	s := New()
	for i, k := range []string{"cold", "warm", "hot"} {
		s.Set(k, []byte("v"))
		setAtime(s, k, uint64(10+i))
	}

	// A read stamps atime through the read lock — the point of the atomic.
	s.clock.Store(9999)
	if _, ok := s.Get("cold"); !ok {
		t.Fatal("Get failed")
	}

	s.SetMemoryLimit(2*entrySize("cold", []byte("v")), PolicyAllKeysLRU)
	if _, ok := s.EnforceMemoryLimit(); !ok {
		t.Fatal("could not get under the limit")
	}
	if _, found := s.Get("cold"); !found {
		t.Error("a key that was just read was evicted anyway")
	}
}

func TestVolatileLRUOnlyEvictsKeysWithDeadlines(t *testing.T) {
	s := New()
	s.Set("permanent", []byte("v"))
	s.SetWithTTL("cache-1", []byte("v"), time.Hour)
	s.SetWithTTL("cache-2", []byte("v"), time.Hour)
	setAtime(s, "cache-1", 1)
	setAtime(s, "cache-2", 2)

	s.SetMemoryLimit(2*entrySize("permanent", []byte("v")), PolicyVolatileLRU)
	evicted, ok := s.EnforceMemoryLimit()
	if !ok || evicted != 1 {
		t.Fatalf("evicted %d (ok=%v), want 1", evicted, ok)
	}
	if _, found := s.Get("permanent"); !found {
		t.Error("volatile-lru evicted a key with no deadline")
	}
	if _, found := s.Get("cache-1"); found {
		t.Error("the oldest volatile key survived")
	}
}

// With nothing left that the policy is allowed to drop, the honest answer is to
// refuse the write — not to start evicting keys the operator said to keep.
func TestVolatileLRURefusesWhenNothingIsVolatile(t *testing.T) {
	s := New()
	s.Set("a", []byte("1"))
	s.Set("b", []byte("2"))
	s.SetMemoryLimit(entrySize("a", []byte("1")), PolicyVolatileLRU)

	if _, ok := s.EnforceMemoryLimit(); ok {
		t.Error("reported success with no volatile keys to evict")
	}
	if n := s.Len(); n != 2 {
		t.Errorf("evicted a permanent key: %d left of 2", n)
	}
}

// Evictions must reach the journal, unlike expirations: nothing in the log
// implies them, so a restart would faithfully restore every evicted key.
func TestEvictionsAreJournalled(t *testing.T) {
	s, r := journalled(t)
	for i, k := range []string{"a", "b", "c"} {
		s.Set(k, []byte("v"))
		setAtime(s, k, uint64(50+i*10))
	}
	setAtime(s, "a", 1) // the oldest, and so the one that must go
	s.SetMemoryLimit(2*entrySize("a", []byte("v")), PolicyAllKeysLRU)

	if _, ok := s.EnforceMemoryLimit(); !ok {
		t.Fatal("could not get under the limit")
	}
	recs := r.all()
	if last := recs[len(recs)-1]; last != "DEL a" {
		t.Errorf("journal ends with %q, want %q", last, "DEL a")
	}
}

func TestEvictionCountsAreReported(t *testing.T) {
	s := New()
	for i := 0; i < 10; i++ {
		s.Set(fmt.Sprintf("k:%d", i), []byte("v"))
	}
	s.SetMemoryLimit(5*entrySize("k:0", []byte("v")), PolicyAllKeysRandom)
	if _, ok := s.EnforceMemoryLimit(); !ok {
		t.Fatal("could not get under the limit")
	}
	if got := s.Stats().EvictedKeys.Load(); got != 5 {
		t.Errorf("evicted_keys = %d, want 5", got)
	}
}

// Is approximated LRU actually worth its cost over picking at random? This
// simulates a cache under a workload with locality — 20% of the keys take 80%
// of the reads — and compares hit rates. If LRU cannot beat random here, the
// sampling is not doing its job and the extra field per entry is not paying for
// itself.
//
// The clock is driven explicitly (one tick per access) rather than left to wall
// time: the real clock refreshes every 100ms, so a simulation running in
// microseconds would give every key an identical timestamp and measure nothing.
// That is a real property of the design, not a testing artefact — see
// DECISIONS.md.
func TestLRUBeatsRandomOnAWorkloadWithLocality(t *testing.T) {
	const (
		keyspace = 2000
		hotKeys  = 400 // 20% of the keyspace...
		hotShare = 0.8 // ...taking 80% of the accesses
		capacity = 500
		accesses = 200_000
	)

	hitRate := func(policy EvictionPolicy) float64 {
		s := New()
		s.SetMemoryLimit(int64(capacity)*entrySize("key:0000", []byte("value")), policy)
		rng := rand.New(rand.NewSource(42)) // same access sequence for both policies

		hits := 0
		for i := 0; i < accesses; i++ {
			s.clock.Store(uint64(i))

			var n int
			if rng.Float64() < hotShare {
				n = rng.Intn(hotKeys)
			} else {
				n = hotKeys + rng.Intn(keyspace-hotKeys)
			}
			key := fmt.Sprintf("key:%04d", n)

			if _, ok := s.Get(key); ok {
				hits++
				continue
			}
			// A miss fills the cache, which is what makes room run out.
			s.EnforceMemoryLimit()
			s.Set(key, []byte("value"))
		}
		return float64(hits) / float64(accesses)
	}

	lru := hitRate(PolicyAllKeysLRU)
	random := hitRate(PolicyAllKeysRandom)
	t.Logf("hit rate: allkeys-lru %.1f%%, allkeys-random %.1f%%", lru*100, random*100)

	if lru <= random {
		t.Errorf("LRU (%.1f%%) did not beat random (%.1f%%); the sampling is not working",
			lru*100, random*100)
	}
}

// Eviction runs while clients are reading and writing. Run with -race: the
// atime store on the read path and the sampler's load on the write path are
// exactly the pair that a non-atomic field would trip over.
func TestEvictionUnderConcurrentLoad(t *testing.T) {
	s := New()
	s.SetMemoryLimit(200*entrySize("key:000", []byte("value")), PolicyAllKeysLRU)

	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("key:%d:%d", g, i%500)
				s.Get(key)
				s.EnforceMemoryLimit()
				s.Set(key, []byte("value"))
			}
		}(g)
	}
	for g := 0; g < 8; g++ {
		<-done
	}

	// The budget is a bound, not a suggestion: allowing one command's overshoot,
	// the keyspace must not have run away.
	if used, max := s.Used(), s.MaxMemory(); used > max+4*entrySize("key:000", []byte("value")) {
		t.Errorf("used %d bytes against a %d byte limit", used, max)
	}
}
