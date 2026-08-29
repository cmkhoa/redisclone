package store

import (
	"fmt"
	"strconv"
	"strings"
)

// Memory bounds and eviction.
//
// Two things have to be true for a byte budget to mean anything: the store has
// to know roughly how much it is using, and it has to have a defensible way to
// pick what to drop. Neither is exact here, and both are approximations by
// choice rather than by neglect — see DECISIONS.md.

// EvictionPolicy says what to do when the keyspace is over its budget.
type EvictionPolicy string

const (
	// PolicyNoEviction refuses writes instead of dropping data. The right
	// default: a store used as a database must not silently lose keys, and a
	// user who wants a cache can say so.
	PolicyNoEviction EvictionPolicy = "noeviction"

	// PolicyAllKeysLRU evicts the least-recently-used key from the whole
	// keyspace. The cache policy people actually want.
	PolicyAllKeysLRU EvictionPolicy = "allkeys-lru"

	// PolicyAllKeysRandom evicts any key at all. Kept because it is the
	// baseline LRU has to beat — and, on a workload with no locality, it is
	// just as good and cheaper.
	PolicyAllKeysRandom EvictionPolicy = "allkeys-random"

	// PolicyVolatileLRU evicts only keys that have a TTL, and refuses the write
	// if there are none. For a keyspace mixing cache entries with data that
	// must not disappear, the TTL is the signal for which is which.
	PolicyVolatileLRU EvictionPolicy = "volatile-lru"
)

func ParseEvictionPolicy(s string) (EvictionPolicy, error) {
	switch p := EvictionPolicy(strings.ToLower(s)); p {
	case PolicyNoEviction, PolicyAllKeysLRU, PolicyAllKeysRandom, PolicyVolatileLRU:
		return p, nil
	default:
		return "", fmt.Errorf("unknown maxmemory-policy %q (want noeviction, allkeys-lru, allkeys-random or volatile-lru)", s)
	}
}

// evictSampleSize is how many keys one eviction decision looks at.
//
// Redis defaults to 5, and the shape of the trade is the interesting part: LRU
// quality rises steeply from 1 (which is random) to about 5, then flattens,
// while cost rises linearly. Sampling 5 of a million keys picks a key in
// roughly the oldest fifth — not the oldest key, and it does not need to be.
const evictSampleSize = 5

// entryOverhead is the estimated per-key cost of everything that is not the key
// and value bytes: the map bucket slot, the string header, the pointer, and the
// entry struct with its deadline and atime.
//
// It is a constant because the alternative — asking the runtime — does not
// work. runtime.ReadMemStats reports heap totals including garbage not yet
// collected, cannot attribute bytes to keys, and stops the world. Redis's
// used_memory is an estimate for the same reason. The number is checked against
// real heap growth in TestMemoryEstimateTracksRealHeapGrowth.
const entryOverhead = 128

func entrySize(key string, val []byte) int64 {
	return int64(len(key)) + int64(len(val)) + entryOverhead
}

// SetMemoryLimit configures the byte budget and what to do when it is reached.
// A limit of 0 means unlimited. Call before serving clients.
func (s *Store) SetMemoryLimit(maxMemory int64, policy EvictionPolicy) {
	s.maxMemory = maxMemory
	s.policy = policy
}

// Used returns the store's estimate of the memory the keyspace occupies.
func (s *Store) Used() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.used
}

// MaxMemory returns the configured budget, 0 if unlimited.
func (s *Store) MaxMemory() int64 { return s.maxMemory }

// Policy returns the configured eviction policy.
func (s *Store) Policy() EvictionPolicy {
	if s.policy == "" {
		return PolicyNoEviction
	}
	return s.policy
}

// EnforceMemoryLimit evicts until the keyspace fits its budget, and reports
// whether it does.
//
// A false return is the server's cue to refuse the write with -OOM. That
// happens under noeviction (which never drops anything), and under the
// volatile policies when nothing is left that is allowed to be dropped.
//
// Called before each write command rather than after: overshooting the budget
// by one command is unavoidable — the size of a write is not known until it
// happens — but overshooting by an unbounded number of them is not.
func (s *Store) EnforceMemoryLimit() (evicted int, ok bool) {
	if s.maxMemory <= 0 {
		return 0, true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.used <= s.maxMemory {
		return 0, true
	}
	if s.Policy() == PolicyNoEviction {
		return 0, false
	}

	for s.used > s.maxMemory {
		key, found := s.pickVictimLocked()
		if !found {
			// Nothing left that this policy is allowed to evict.
			return evicted, false
		}
		s.deleteLocked(key)
		s.stats.EvictedKeys.Add(1)
		evicted++

		// Evictions *must* be journalled, unlike expirations. An expired key
		// is deterministic — the log carries its deadline, so replay reaches
		// the same conclusion on its own. An eviction is a decision this
		// process made about this machine's memory at this moment, and nothing
		// in the log implies it. Without the DEL, restarting would faithfully
		// restore every key we just evicted and blow the budget again.
		s.propagate(cmdDEL, []byte(key))
	}
	return evicted, true
}

// pickVictimLocked chooses a key to evict under the configured policy.
//
// Approximated LRU: sample a handful of keys and evict the oldest of them,
// rather than tracking exact recency. See DECISIONS.md — the short version is
// that exact LRU needs an intrusive linked list whose nodes must be moved on
// every read, which turns every GET into a write-locked operation.
func (s *Store) pickVictimLocked() (string, bool) {
	pool := s.data
	volatile := s.Policy() == PolicyVolatileLRU
	if volatile {
		if len(s.expiring) == 0 {
			return "", false
		}
	} else if len(s.data) == 0 {
		return "", false
	}

	var (
		best      string
		bestAtime uint64
		found     bool
		scanned   int
	)

	// Ranging a map starts at a random bucket, so taking the first few keys is
	// a cheap random sample. The same trick the expiry sampler uses.
	sample := func(key string) bool {
		e, ok := s.data[key]
		if !ok {
			return true // index drift; the expiry sampler repairs it
		}
		scanned++
		if !found || e.atime.Load() < bestAtime {
			best, bestAtime, found = key, e.atime.Load(), true
		}
		return scanned < evictSampleSize
	}

	if volatile {
		for key := range s.expiring {
			if !sample(key) {
				break
			}
		}
	} else if s.Policy() == PolicyAllKeysRandom {
		// One key, no comparison: this policy is the control group.
		for key := range pool {
			return key, true
		}
	} else {
		for key := range pool {
			if !sample(key) {
				break
			}
		}
	}
	return best, found
}

// ParseMemory reads a byte budget written the way a human writes one:
// "100mb", "2gb", "1048576". Case-insensitive; the suffixes are powers of 1024,
// as in redis.conf.
func ParseMemory(s string) (int64, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	mult := int64(1)
	for _, suffix := range []struct {
		s string
		m int64
	}{
		{"kb", 1 << 10}, {"mb", 1 << 20}, {"gb", 1 << 30},
		{"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30},
		{"b", 1},
	} {
		if strings.HasSuffix(t, suffix.s) {
			t, mult = strings.TrimSpace(strings.TrimSuffix(t, suffix.s)), suffix.m
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory size %q", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid memory size %q: negative", s)
	}
	return n * mult, nil
}
