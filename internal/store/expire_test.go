package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Being in the same package as the store is what makes these tests fast and
// deterministic: instead of sleeping until a key expires, they plant an entry
// whose deadline is already in the past. Nothing here waits on the clock except
// the two tests that are specifically about waiting.

// plantExpired inserts a key that expired a second ago, bypassing the public
// API — exactly the state the sampler and the lazy path are supposed to find.
func plantExpired(s *Store, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = &entry{val: []byte("stale"), expiresAt: time.Now().Add(-time.Second)}
	s.expiring[key] = struct{}{}
	s.used += entrySize(key, []byte("stale"))
}

// checkIndex asserts the invariant that ties the two maps together: a key is in
// `expiring` if and only if its entry has a deadline.
func checkIndex(t *testing.T, s *Store) {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, e := range s.data {
		_, indexed := s.expiring[k]
		if e.expiresAt.IsZero() && indexed {
			t.Errorf("key %q has no deadline but is in the expiring index", k)
		}
		if !e.expiresAt.IsZero() && !indexed {
			t.Errorf("key %q has a deadline but is missing from the expiring index", k)
		}
	}
	for k := range s.expiring {
		if _, ok := s.data[k]; !ok {
			t.Errorf("key %q is in the expiring index but not in the keyspace", k)
		}
	}
}

func TestGetCollectsExpiredKey(t *testing.T) {
	s := New()
	plantExpired(s, "k")

	if _, ok := s.Get("k"); ok {
		t.Fatal("Get returned an expired key")
	}
	// Lazy expiration is not just a filter: the key is gone, not merely hidden.
	if n := s.Len(); n != 0 {
		t.Errorf("Len = %d after reading an expired key, want 0", n)
	}
	checkIndex(t, s)
}

func TestExistsAndDelIgnoreExpiredKeys(t *testing.T) {
	s := New()
	s.Set("live", []byte("v"))
	plantExpired(s, "dead")

	if n := s.Exists("live", "dead"); n != 1 {
		t.Errorf("Exists = %d, want 1 (the expired key must not count)", n)
	}
	// DEL reports keys it actually removed *from the client's point of view*,
	// and an expired key was already gone as far as the client knows.
	if n := s.Del("live", "dead"); n != 1 {
		t.Errorf("Del = %d, want 1", n)
	}
	if n := s.Len(); n != 0 {
		t.Errorf("Len = %d, want 0 — DEL should still physically remove the expired key", n)
	}
	checkIndex(t, s)
}

func TestSetClearsTTL(t *testing.T) {
	s := New()
	s.SetWithTTL("k", []byte("v"), time.Hour)
	if _, state := s.TTL("k"); state != KeyVolatile {
		t.Fatalf("TTL state = %v, want KeyVolatile", state)
	}

	s.Set("k", []byte("v2"))
	if _, state := s.TTL("k"); state != KeyPersistent {
		t.Errorf("TTL state after a plain Set = %v, want KeyPersistent", state)
	}
	checkIndex(t, s)
}

func TestSetWithNonPositiveTTLDeletes(t *testing.T) {
	s := New()
	s.Set("k", []byte("old"))
	s.SetWithTTL("k", []byte("new"), -time.Second)

	if _, ok := s.Get("k"); ok {
		t.Error("a non-positive TTL should have removed the key")
	}
	checkIndex(t, s)
}

func TestExpire(t *testing.T) {
	t.Run("on a live key", func(t *testing.T) {
		s := New()
		s.Set("k", []byte("v"))
		if !s.Expire("k", time.Hour) {
			t.Fatal("Expire on an existing key returned false")
		}
		d, state := s.TTL("k")
		if state != KeyVolatile {
			t.Fatalf("state = %v, want KeyVolatile", state)
		}
		if d <= 59*time.Minute || d > time.Hour {
			t.Errorf("remaining = %v, want just under an hour", d)
		}
		checkIndex(t, s)
	})

	t.Run("on a missing key", func(t *testing.T) {
		s := New()
		if s.Expire("nope", time.Hour) {
			t.Error("Expire on a missing key returned true")
		}
	})

	t.Run("on an expired key", func(t *testing.T) {
		s := New()
		plantExpired(s, "k")
		if s.Expire("k", time.Hour) {
			t.Error("Expire resurrected an expired key")
		}
		if n := s.Len(); n != 0 {
			t.Errorf("Len = %d, want 0 — Expire holds the write lock, so it should collect", n)
		}
		checkIndex(t, s)
	})

	t.Run("with a non-positive ttl deletes", func(t *testing.T) {
		s := New()
		s.Set("k", []byte("v"))
		if !s.Expire("k", -time.Second) {
			t.Error("Expire with a past deadline returned false; the key was there")
		}
		if _, ok := s.Get("k"); ok {
			t.Error("Expire with a past deadline left the key behind")
		}
		checkIndex(t, s)
	})

	t.Run("replaces an existing deadline", func(t *testing.T) {
		s := New()
		s.SetWithTTL("k", []byte("v"), time.Minute)
		s.Expire("k", time.Hour)
		d, _ := s.TTL("k")
		if d < 59*time.Minute {
			t.Errorf("remaining = %v, want the new hour-long deadline", d)
		}
	})
}

func TestTTLStates(t *testing.T) {
	s := New()
	s.Set("persistent", []byte("v"))
	s.SetWithTTL("volatile", []byte("v"), time.Hour)
	plantExpired(s, "expired")

	tests := []struct {
		key  string
		want TTLState
	}{
		{"missing", KeyMissing},
		{"expired", KeyMissing}, // expired is indistinguishable from missing
		{"persistent", KeyPersistent},
		{"volatile", KeyVolatile},
	}
	for _, tt := range tests {
		if _, got := s.TTL(tt.key); got != tt.want {
			t.Errorf("TTL(%q) state = %v, want %v", tt.key, got, tt.want)
		}
	}
}

// The race the lazy path is built around: Get finds an expired key, drops the
// read lock to take the write lock, and in that window another client SETs the
// key afresh. Without the re-check under the write lock, Get deletes a value
// that is very much alive.
//
// Either interleaving has one correct answer — the freshly set value survives —
// so this is deterministic in outcome even though it is a race in timing. Run
// enough iterations and a missing re-check loses.
func TestLazyExpiryDoesNotDeleteAResurrectedKey(t *testing.T) {
	s := New()
	plantExpired(s, "k")

	// Resurrect the key from inside the window, which is what a concurrent
	// client's SET would do. Racing two goroutines instead would pass against
	// broken code almost every run: the window is a few hundred nanoseconds
	// wide, and a test that fails one run in ten thousand is not a test.
	testHookExpiredWindow = func() {
		testHookExpiredWindow = nil // fire once
		s.Set("k", []byte("fresh"))
	}
	t.Cleanup(func() { testHookExpiredWindow = nil })

	// This Get read the key while it was still expired, so it must report it
	// missing...
	if _, ok := s.Get("k"); ok {
		t.Error("Get returned a value for a key that was expired when it read it")
	}
	// ...but the value written during the window has to survive, because Get
	// re-checks the deadline under the write lock and finds it no longer past.
	v, ok := s.Get("k")
	if !ok || string(v) != "fresh" {
		t.Fatalf("freshly set key was collected by lazy expiry (got %q, found=%v)", v, ok)
	}
	checkIndex(t, s)
}

func TestActiveExpireCycleCollectsExpiredKeys(t *testing.T) {
	s := New()
	const live, dead = 50, 200
	for i := 0; i < live; i++ {
		s.Set(fmt.Sprintf("live:%d", i), []byte("v"))
	}
	for i := 0; i < dead; i++ {
		plantExpired(s, fmt.Sprintf("dead:%d", i))
	}

	// One cycle samples 20 keys per round and stops early once a round comes
	// back mostly live, so it is bounded — it is not supposed to clear
	// everything at once. Run cycles until it converges, as the ticker would.
	total := 0
	for i := 0; i < 100; i++ {
		_, expired := s.activeExpireCycle()
		total += expired
		if len(s.expiring) == 0 {
			break
		}
	}

	if total != dead {
		t.Errorf("collected %d expired keys, want %d", total, dead)
	}
	if n := s.Len(); n != live {
		t.Errorf("Len = %d, want %d — the sampler deleted live keys", n, live)
	}
	for i := 0; i < live; i++ {
		if _, ok := s.Get(fmt.Sprintf("live:%d", i)); !ok {
			t.Fatalf("live:%d was collected", i)
		}
	}
	checkIndex(t, s)
}

// A cycle over a keyspace with nothing expired must stop after one round: the
// hit rate is zero, so there is no reason to sample again. This is what keeps
// the sampler off the CPU when nothing is expiring.
func TestActiveExpireCycleStopsWhenNothingIsExpiring(t *testing.T) {
	s := New()
	for i := 0; i < 100; i++ {
		s.SetWithTTL(fmt.Sprintf("k:%d", i), []byte("v"), time.Hour)
	}

	scanned, expired := s.activeExpireCycle()
	if expired != 0 {
		t.Errorf("expired = %d, want 0", expired)
	}
	if scanned > activeExpireSampleSize {
		t.Errorf("scanned = %d keys, want at most one round of %d", scanned, activeExpireSampleSize)
	}
}

// Persistent keys must never be sampled at all — that is the entire reason the
// expiring index exists. A million persistent keys and one volatile key should
// still find the volatile one immediately.
func TestActiveExpireCycleOnlySamplesVolatileKeys(t *testing.T) {
	s := New()
	for i := 0; i < 10_000; i++ {
		s.Set(fmt.Sprintf("persistent:%d", i), []byte("v"))
	}
	plantExpired(s, "needle")

	scanned, expired := s.activeExpireCycle()
	if scanned != 1 {
		t.Errorf("scanned %d keys, want exactly the 1 volatile key", scanned)
	}
	if expired != 1 {
		t.Errorf("expired = %d, want 1 — the needle was not collected", expired)
	}
	if n := s.Len(); n != 10_000 {
		t.Errorf("Len = %d, want the 10000 persistent keys untouched", n)
	}
}

// End to end through the real ticker: a key with a short TTL that nobody ever
// reads must still be released. This is the one test that has to wait, because
// it is testing that something happens without being asked.
func TestRunActiveExpirationReleasesUnreadKeys(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); s.RunActiveExpiration(ctx, 5*time.Millisecond) }()

	for i := 0; i < 100; i++ {
		s.SetWithTTL(fmt.Sprintf("k:%d", i), []byte("v"), 10*time.Millisecond)
	}

	deadline := time.Now().Add(2 * time.Second)
	for s.Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := s.Len(); n != 0 {
		t.Errorf("%d keys still held after 2s; the sampler is not collecting", n)
	}

	// And it stops when told to.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("RunActiveExpiration did not return after its context was cancelled")
	}
}

// The two maps are written by six different methods. Hammer all of them from
// several goroutines and assert the index invariant still holds. Run with -race.
func TestExpiringIndexStaysConsistentUnderLoad(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				k := fmt.Sprintf("k:%d:%d", g, i%10)
				switch i % 6 {
				case 0:
					s.Set(k, []byte("v"))
				case 1:
					s.SetWithTTL(k, []byte("v"), time.Duration(i%3)*time.Millisecond)
				case 2:
					s.Expire(k, time.Hour)
				case 3:
					s.Expire(k, -time.Second)
				case 4:
					s.Del(k)
				case 5:
					s.Get(k)
					s.TTL(k)
					s.activeExpireCycle()
				}
			}
		}(g)
	}
	wg.Wait()
	checkIndex(t, s)
}
