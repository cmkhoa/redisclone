package store

import (
	"context"
	"time"
)

// Active expiration: deleting keys whose deadline has passed but which nobody
// has looked at.
//
// Lazy expiration alone is correct but unbounded — a key SET with a TTL and
// never read again would hold its memory forever. Scanning the whole keyspace
// on a timer would be bounded but would stall every client for as long as the
// scan takes, which on a large keyspace is exactly the latency spike a cache
// exists to avoid.
//
// The way out is Redis's: sample a few volatile keys at random, delete the
// expired ones, and use the hit rate to decide whether to keep going. If a
// quarter of the sample was expired there is probably more where that came
// from, so go round again; if not, stop and let the next tick handle it. That
// bounds the work per cycle at a constant, keeps every critical section
// microseconds long, and converges quickly on the case that matters (a big
// batch of keys expiring at once) without paying for it when nothing is
// expiring.
//
// What it buys with that is a bound on *staleness*, not a guarantee: memory for
// an expired key is released soon, not immediately. No client can observe the
// difference, because the lazy path already makes an expired key invisible.

const (
	// activeExpireSampleSize is how many volatile keys one round looks at.
	// Redis uses 20. Small enough that a round holds the write lock for
	// microseconds; large enough that the hit rate below means something.
	activeExpireSampleSize = 20

	// activeExpireThreshold is the fraction of a sample that must be expired
	// to justify another round immediately.
	activeExpireThreshold = 0.25

	// activeExpireMaxRounds caps one cycle even when every sample is expired,
	// so a keyspace where everything expires at once cannot monopolise the
	// lock. The leftovers wait for the next tick.
	activeExpireMaxRounds = 16

	// DefaultActiveExpireInterval is how often a cycle runs. Redis's default
	// is 10 Hz, and the reasoning carries over: often enough that memory is
	// released promptly, rare enough to be invisible in a profile.
	DefaultActiveExpireInterval = 100 * time.Millisecond
)

// RunActiveExpiration runs expiry cycles until ctx is cancelled. Intended to be
// run in its own goroutine; it returns when ctx is done.
func (s *Store) RunActiveExpiration(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultActiveExpireInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.activeExpireCycle()
		}
	}
}

// activeExpireCycle runs rounds of sampling until one comes back mostly clean,
// or until the round cap is hit. Returns the totals, which the tests use.
func (s *Store) activeExpireCycle() (scanned, expired int) {
	for round := 0; round < activeExpireMaxRounds; round++ {
		n, gone := s.expireSample(activeExpireSampleSize)
		scanned += n
		expired += gone

		// Nothing volatile left to look at, or the sample was mostly live:
		// either way there is no reason to believe another round would pay.
		if n == 0 || float64(gone)/float64(n) < activeExpireThreshold {
			break
		}
	}
	return scanned, expired
}

// expireSample looks at up to n volatile keys and deletes the expired ones,
// returning how many it looked at and how many it deleted.
//
// It takes the write lock for the whole sample and releases it before
// returning: one short exclusive section per round, with the caller free to
// let other goroutines in between rounds. Holding the lock across a whole
// cycle would be simpler and would reintroduce the stall this design exists to
// avoid.
func (s *Store) expireSample(n int) (scanned, expired int) {
	now := time.Now()

	// This cycle is the metronome for the coarse LRU clock: it runs on a timer
	// and is already paying for a time.Now, so the read path never has to.
	s.tick()

	// Ranging a Go map starts at a randomly chosen bucket and offset, so
	// breaking out after n keys is a cheap random-ish sample rather than
	// always the same n keys. It is not a uniform random draw — keys sharing a
	// bucket are sampled together — but the algorithm only needs "different
	// keys each time", which it does give.
	//
	// Deleting from a map while ranging over it is defined behaviour in Go:
	// entries deleted before they are reached are simply not produced.
	for i := range s.readShards {
		if scanned == n {
			break
		}
		shard := &s.readShards[i]
		shard.mu.Lock()
		for key := range shard.expiring {
			if scanned == n {
				break
			}
			scanned++
			e, ok := shard.data[key]
			if !ok {
				delete(shard.expiring, key)
				continue
			}
			if e.expired(now) {
				shard.used -= entrySize(key, e.val)
				delete(shard.data, key)
				delete(shard.expiring, key)
				s.stats.ExpiredKeys.Add(1)
				expired++
			}
		}
		shard.mu.Unlock()
	}
	return scanned, expired
}
