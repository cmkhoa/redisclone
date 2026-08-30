// Package store holds the keyspace: a concurrency-safe map from keys to
// values, some of which have a deadline.
//
// The one invariant that makes everything else here safe:
//
//	A value handed to Set, or returned by Get, is never mutated in place.
//
// Set takes ownership of the slice it is given (callers must not keep writing
// to it), and Get returns the live slice rather than a copy. That works
// because an overwrite replaces the map entry with a different slice instead of
// writing over the old bytes, so a reader that took a slice out of the map
// before the overwrite keeps reading valid, if now-stale, data. Any future
// command that wants to modify a value in place (APPEND, SETRANGE) must
// copy-on-write instead.
//
// # Expiration
//
// A key with a deadline is "volatile". Deadlines are enforced two ways, and
// both are necessary:
//
//   - Lazily, on every read: an expired key reports as missing and is deleted
//     on the spot. This is what makes expiry *correct* — no client ever sees a
//     key that should be gone.
//   - Actively, by a background sampler (RunActiveExpiration): it deletes
//     expired keys nobody has asked for. This is what makes expiry *bounded* —
//     without it, a key written once with a TTL and never read again occupies
//     memory forever.
//
// Volatile keys are additionally tracked in their own index (see Store.expiring)
// so the sampler has something small to sample from.
package store

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// entry is one key's value plus its deadline.
//
// A struct value in the map rather than a pointer: no per-key allocation and
// no indirection on the read path, at the cost of copying 40 bytes per lookup
// and of not being able to assign to a field in place (m[k].val = v does not
// compile — the whole entry has to be reassigned, which the write paths do).
type entry struct {
	val []byte
	// atime is a coarse logical clock.  It is atomic because GET updates it
	// while holding only the read lock.
	atime atomic.Uint64
	// expiresAt is the zero Time for a key that never expires.
	//
	// A time.Time rather than a Unix timestamp because time.Now() carries a
	// monotonic reading, and comparisons between two such Times use it — so
	// deadlines survive the wall clock being stepped by NTP or by an admin.
	// The cost is 16 bytes per entry over an int64, and one conversion when
	// M3's AOF has to write an absolute wall-clock time to disk.
	expiresAt time.Time
}

// expired reports whether e's deadline has passed as of now. A deadline
// exactly reached counts as expired, matching Redis.
func (e *entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// TTLState distinguishes the three answers TTL can give, which RESP encodes as
// three different integers (-2, -1, and the remaining time).
type TTLState int

const (
	// KeyMissing: no such key.
	KeyMissing TTLState = iota
	// KeyPersistent: the key exists and has no deadline.
	KeyPersistent
	// KeyVolatile: the key exists and expires; the returned duration is what
	// is left of it.
	KeyVolatile
)

// Journal records mutations durably so they can be replayed after a restart.
//
// The store calls Append while holding the keyspace write lock. That is not an
// accident and not negotiable: it is what guarantees the journal's order is the
// order the mutations actually happened in. Two clients racing to SET the same
// key are ordered by this lock; if the journal were appended after the lock was
// released, the loser could reach the file second-to-last and replay would
// rebuild a keyspace that never existed.
//
// The consequence for implementers: Append must be cheap and must not block on
// I/O. aof.Log satisfies that by encoding into a buffer and leaving the write
// and fsync to a caller that no longer holds the lock.
type Journal interface {
	Append(args ...[]byte) error
}

// Store is a keyspace. The zero value is not usable; call New.
//
// Safe for concurrent use by any number of goroutines — which it has to be,
// because the server runs one goroutine per connection and therefore runs
// commands from different clients at the same time.
type Store struct {
	// One RWMutex over the whole store. Reads (GET, EXISTS, TTL) share it;
	// writes (SET, DEL, EXPIRE, and the expiry sampler) take it exclusively.
	// See DECISIONS.md for why this rather than a sharded map or sync.Map —
	// briefly: this is the version whose correctness is obvious, and M5 is
	// where measurements get to argue for something cleverer.
	mu sync.RWMutex

	data map[string]*entry

	// expiring indexes the volatile keys — exactly the keys whose entry has a
	// non-zero expiresAt.
	//
	// It exists for the sampler, which has to find expired keys without
	// scanning the keyspace. Sampling `data` directly would work when most
	// keys are volatile and degenerate to uselessness when few are: with a
	// million persistent keys and ten volatile ones, twenty random samples
	// almost never touch a key that can expire. Redis keeps a second dict for
	// the same reason.
	//
	// The cost is that every write path has to keep two maps consistent;
	// TestExpiringIndexStaysConsistent is what stops that being a slow leak.
	expiring map[string]struct{}

	// journal is nil when durability is off — and during replay, which is what
	// stops a restart from rewriting everything it has just read.
	journal Journal

	// M4's accounting and eviction state. maxMemory and policy are configured
	// before clients are accepted; used is always protected by mu.
	maxMemory int64
	policy    EvictionPolicy
	used      int64
	clock     atomic.Uint64
	stats     Stats
}

// Stats are monotonically increasing counters exposed by INFO.
type Stats struct {
	KeyspaceHits   atomic.Uint64
	KeyspaceMisses atomic.Uint64
	EvictedKeys    atomic.Uint64
	ExpiredKeys    atomic.Uint64
}

func New() *Store {
	return &Store{
		data:     make(map[string]*entry),
		expiring: make(map[string]struct{}),
	}
}

// SetJournal attaches a journal. Call it before serving clients: it is not
// synchronised, because a journal appearing mid-flight would leave the log
// missing everything that came before it.
func (s *Store) SetJournal(j Journal) { s.journal = j }

// propagate records one mutation. Called with the write lock held.
//
// The error is deliberately dropped: there is nothing useful the store can do
// about a failed disk write, and the mutation has already happened in memory.
// The log latches the failure instead, and the server refuses subsequent write
// commands with -MISCONF rather than accepting writes it cannot persist.
func (s *Store) propagate(args ...[]byte) {
	if s.journal != nil {
		_ = s.journal.Append(args...)
	}
}

// Command names as they appear in the journal.
var (
	cmdSET       = []byte("SET")
	cmdMSET      = []byte("MSET")
	cmdDEL       = []byte("DEL")
	cmdPXAT      = []byte("PXAT")
	cmdPEXPIREAT = []byte("PEXPIREAT")
)

// unixMillis formats t the way the journal carries deadlines: an absolute
// wall-clock time, because a relative one means something different when it is
// replayed an hour later.
func unixMillis(t time.Time) []byte {
	return strconv.AppendInt(nil, t.UnixMilli(), 10)
}

// Get returns the value stored at key and whether it existed.
//
// An expired key reports as missing and is deleted on the way past (lazy
// expiration).
//
// The returned slice aliases the stored value: read it, don't write it.
func (s *Store) Get(key string) ([]byte, bool) {
	now := time.Now()

	s.mu.RLock()
	e, ok := s.data[key]
	expired := ok && e.expired(now)
	s.mu.RUnlock()

	switch {
	case !ok:
		s.stats.KeyspaceMisses.Add(1)
		return nil, false
	case !expired:
		e.atime.Store(s.clock.Load())
		s.stats.KeyspaceHits.Add(1)
		return e.val, true
	}

	// The window this hook opens is the whole point of the re-check below; a
	// test drives it deterministically because it is far too narrow to hit by
	// chance. Nil in production, and reached only on the already-slow path.
	if testHookExpiredWindow != nil {
		testHookExpiredWindow()
	}

	// The key is expired, so it has to go — but deleting needs the write lock,
	// and an RWMutex cannot be upgraded. Dropping the read lock and taking the
	// write lock opens a window in which another client can SET this key
	// afresh, so the deadline is re-checked (against a *new* now) before
	// anything is deleted. Skipping that re-check is how a lazy-expiry
	// implementation ends up deleting live data under load.
	s.mu.Lock()
	if e, ok := s.data[key]; ok && e.expired(time.Now()) {
		s.deleteLocked(key)
	}
	s.mu.Unlock()
	return nil, false
}

// Set stores val at key, replacing any existing value and clearing any
// deadline, and takes ownership of val.
//
// Clearing the TTL is Redis's behaviour: a plain SET makes the key persistent
// again. (KEEPTTL, when it exists, will be the opt-out.)
//
// A nil val is stored as an empty value — Redis has no "null" value, only a
// missing key.
func (s *Store) Set(key string, val []byte) {
	s.set(key, val, time.Time{})
}

// SetWithTTL stores val at key with a deadline ttl from now. A ttl of zero or
// less deletes the key instead: the caller is asking for a value that is
// already stale.
func (s *Store) SetWithTTL(key string, val []byte, ttl time.Duration) {
	if ttl <= 0 {
		s.Del(key)
		return
	}
	s.set(key, val, time.Now().Add(ttl))
}

// SetWithDeadline stores val at key expiring at an absolute time — what SET's
// PXAT/EXAT options ask for, and what the journal replays.
//
// A deadline already in the past removes the key: replaying a log written
// yesterday must not resurrect what expired overnight.
func (s *Store) SetWithDeadline(key string, val []byte, at time.Time) {
	if !at.After(time.Now()) {
		s.Del(key)
		return
	}
	s.set(key, val, at)
}

func (s *Store) set(key string, val []byte, expiresAt time.Time) {
	s.mu.Lock()
	s.setLocked(key, val, expiresAt)
	s.mu.Unlock()
}

// setLocked replaces key while preserving the accounting/index invariants.
// The caller holds s.mu.
func (s *Store) setLocked(key string, val []byte, expiresAt time.Time) {
	if val == nil {
		val = []byte{}
	}
	if old, ok := s.data[key]; ok {
		s.used -= entrySize(key, old.val)
	}
	e := &entry{val: val, expiresAt: expiresAt}
	e.atime.Store(s.clock.Load())
	s.data[key] = e
	s.used += entrySize(key, val)
	if expiresAt.IsZero() {
		delete(s.expiring, key)
	} else {
		s.expiring[key] = struct{}{}
	}
	s.propagateSetLocked(key, val, expiresAt)
}

// propagateSetLocked records a full value replacement. The caller holds s.mu.
func (s *Store) propagateSetLocked(key string, val []byte, expiresAt time.Time) {
	if expiresAt.IsZero() {
		s.propagate(cmdSET, []byte(key), val)
		return
	}
	// One record, not SET plus PEXPIREAT: a torn tail must not create a value
	// that was supposed to expire forever.
	s.propagate(cmdSET, []byte(key), val, cmdPXAT, unixMillis(expiresAt))
}

// Pair is one key/value assignment for MSET.
type Pair struct {
	Key   string
	Value []byte
}

// MSet atomically applies all assignments. A later duplicate key wins, like
// Redis. Plain assignments clear any previous TTL.
func (s *Store) MSet(pairs []Pair) {
	s.mu.Lock()
	args := make([][]byte, 1, 1+2*len(pairs))
	args[0] = cmdMSET
	for _, p := range pairs {
		s.setLockedNoJournal(p.Key, p.Value, time.Time{})
		args = append(args, []byte(p.Key), p.Value)
	}
	s.propagate(args...)
	s.mu.Unlock()
}

func (s *Store) setLockedNoJournal(key string, val []byte, expiresAt time.Time) {
	if val == nil {
		val = []byte{}
	}
	if old, ok := s.data[key]; ok {
		s.used -= entrySize(key, old.val)
	}
	e := &entry{val: val, expiresAt: expiresAt}
	e.atime.Store(s.clock.Load())
	s.data[key] = e
	s.used += entrySize(key, val)
	if expiresAt.IsZero() {
		delete(s.expiring, key)
	} else {
		s.expiring[key] = struct{}{}
	}
}

var ErrNotInteger = errors.New("value is not an integer or out of range")

// Increment adds delta to key's integer value. It uses copy-on-write and
// preserves a live key's TTL.
func (s *Store) Increment(key string, delta int64) (int64, error) {
	now := time.Now()
	s.mu.Lock()
	e, ok := s.data[key]
	if ok && e.expired(now) {
		s.deleteLocked(key)
		ok = false
	}
	var n int64
	if ok {
		var err error
		n, err = strconv.ParseInt(string(e.val), 10, 64)
		if err != nil {
			s.mu.Unlock()
			return 0, ErrNotInteger
		}
	}
	if (delta > 0 && n > int64(^uint64(0)>>1)-delta) || (delta < 0 && n < -int64(^uint64(0)>>1)-1-delta) {
		s.mu.Unlock()
		return 0, ErrNotInteger
	}
	n += delta
	var expiresAt time.Time
	if ok {
		expiresAt = e.expiresAt
	}
	val := strconv.AppendInt(nil, n, 10)
	s.setLocked(key, val, expiresAt)
	s.mu.Unlock()
	return n, nil
}

// Append adds suffix to key and returns the resulting length. It never mutates
// the existing value, because GET may have handed that slice to another client.
func (s *Store) Append(key string, suffix []byte) int {
	now := time.Now()
	s.mu.Lock()
	e, ok := s.data[key]
	if ok && e.expired(now) {
		s.deleteLocked(key)
		ok = false
	}
	var old []byte
	var expiresAt time.Time
	if ok {
		old, expiresAt = e.val, e.expiresAt
	}
	val := make([]byte, len(old)+len(suffix))
	copy(val, old)
	copy(val[len(old):], suffix)
	s.setLocked(key, val, expiresAt)
	s.mu.Unlock()
	return len(val)
}

// Expire gives key a deadline ttl from now, replacing any existing one, and
// reports whether the key was there to be given one.
//
// A ttl of zero or less deletes the key and reports true, which is what Redis
// does: the client asked for the key to be gone by a time that has already
// passed, and the honest way to honour that is to remove it now rather than
// leave a key behind whose TTL says -1.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	return s.ExpireAt(key, time.Now().Add(ttl))
}

// ExpireAt gives key an absolute deadline, replacing any existing one, and
// reports whether the key was there to be given one. A deadline in the past
// deletes it.
//
// This is the form the journal carries, for the same reason SET does: "expires
// in 60 seconds" is not a fact that survives being written down.
func (s *Store) ExpireAt(key string, at time.Time) bool {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || e.expired(now) {
		// Already gone as far as any client is concerned. Collect it while we
		// are here, holding the write lock anyway.
		if ok {
			s.deleteLocked(key)
			s.propagate(cmdDEL, []byte(key))
		}
		return false
	}
	if !at.After(now) {
		s.deleteLocked(key)
		s.propagate(cmdDEL, []byte(key))
		return true
	}
	e.expiresAt = at
	s.expiring[key] = struct{}{}
	s.propagate(cmdPEXPIREAT, []byte(key), unixMillis(at))
	return true
}

// TTL returns how long key has left, and which of the three cases applies. The
// duration is meaningful only for KeyVolatile.
func (s *Store) TTL(key string) (time.Duration, TTLState) {
	now := time.Now()

	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()

	switch {
	case !ok, e.expired(now):
		// Deliberately does not collect the expired key: TTL is a read, and
		// taking the write lock on a read path is a cost every reader pays for
		// a cleanup the sampler will do anyway. Get collects because it has to
		// re-check under the write lock regardless; TTL has no such need.
		return 0, KeyMissing
	case e.expiresAt.IsZero():
		return 0, KeyPersistent
	default:
		return e.expiresAt.Sub(now), KeyVolatile
	}
}

// Del removes keys and returns how many were actually there. An already-expired
// key is removed but not counted: it was gone before the client asked.
//
// One lock acquisition for the whole batch, not one per key: it is cheaper, and
// it makes a multi-key DEL atomic with respect to other commands, which is what
// clients expect of a single command.
func (s *Store) Del(keys ...string) int {
	now := time.Now()
	n := 0

	s.mu.Lock()
	// The journal records what was removed, not what was asked for: a DEL of
	// three keys where only one existed replays as a one-key DEL. Keeping the
	// log to actual effects is what makes replay independent of the state the
	// keyspace happened to be in when the command ran.
	removed := make([][]byte, 0, len(keys)+1)
	removed = append(removed, cmdDEL)
	for _, k := range keys {
		e, ok := s.data[k]
		if !ok {
			continue
		}
		s.deleteLocked(k)
		removed = append(removed, []byte(k))
		if !e.expired(now) {
			n++
		}
	}
	if len(removed) > 1 {
		s.propagate(removed...)
	}
	s.mu.Unlock()
	return n
}

// Exists returns how many of keys are present, counting duplicates: EXISTS on
// the same existing key three times returns 3, as in real Redis. Expired keys
// do not count.
func (s *Store) Exists(keys ...string) int {
	now := time.Now()
	n := 0

	s.mu.RLock()
	for _, k := range keys {
		if e, ok := s.data[k]; ok && !e.expired(now) {
			n++
		}
	}
	s.mu.RUnlock()
	return n
}

// Len returns the number of keys physically present, including expired ones the
// sampler has not collected yet.
//
// That makes it the wrong number for DBSIZE, which has to report what clients
// can see. It is the right number for tests and for M4's memory accounting,
// both of which care about what is actually held.
func (s *Store) Len() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}

// DBSize is the number of client-visible keys. Expired entries that have not
// yet been sampled are deliberately excluded.
func (s *Store) DBSize() int {
	now := time.Now()
	s.mu.RLock()
	n := 0
	for _, e := range s.data {
		if !e.expired(now) {
			n++
		}
	}
	s.mu.RUnlock()
	return n
}

// Stats returns the server counters. Callers load the atomic fields they need.
func (s *Store) Stats() *Stats { return &s.stats }

// tick advances the coarse LRU clock. The expiry ticker calls it every 100ms,
// avoiding a clock read/write on every GET while retaining useful recency.
func (s *Store) tick() { s.clock.Add(1) }

// testHookExpiredWindow, when non-nil, is called by Get after it has released
// the read lock on an expired key and before it takes the write lock — the
// exact window in which another client can resurrect that key with SET. See
// TestLazyExpiryDoesNotDeleteAResurrectedKey.
var testHookExpiredWindow func()

// deleteLocked removes a key from both maps. The caller holds the write lock.
//
// Every deletion in this package goes through here — that is the whole defence
// against the two maps drifting apart.
func (s *Store) deleteLocked(key string) {
	if e, ok := s.data[key]; ok {
		s.used -= entrySize(key, e.val)
	}
	delete(s.data, key)
	delete(s.expiring, key)
}
