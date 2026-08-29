package server

import (
	"fmt"
	"io"
	"log"
	"strings"
	"testing"

	"redisclone/internal/store"
)

func limited(t *testing.T, bytes int64, policy store.EvictionPolicy) *Server {
	t.Helper()
	s := New(log.New(io.Discard, "", 0))
	s.Store().SetMemoryLimit(bytes, policy)
	return s
}

// Under noeviction the server refuses writes rather than dropping data — and
// keeps serving reads, because a read-only cache is far more useful than a dead
// one.
func TestOOMRefusesWritesButNotReads(t *testing.T) {
	s := limited(t, 1, store.PolicyNoEviction)
	run(t, s, cmd("SET", "existing", "v")) // the write that goes over the line

	for _, c := range []string{
		cmd("SET", "k", "v"), cmd("DEL", "existing"), cmd("EXPIRE", "existing", "10"),
	} {
		if got := run(t, s, c); !strings.HasPrefix(got, "-OOM") {
			t.Errorf("%q replied %q, want -OOM", c, got)
		}
	}

	want := "$1\r\nv\r\n" + ":1\r\n" + "+PONG\r\n"
	if got := run(t, s, cmd("GET", "existing")+cmd("EXISTS", "existing")+cmd("PING")); got != want {
		t.Errorf("reads stopped working: %q, want %q", got, want)
	}
}

// With a policy that allows eviction, writes keep succeeding and the keyspace
// stays inside its budget.
func TestEvictionKeepsWritesWorking(t *testing.T) {
	const capacity = 50
	s := limited(t, capacity*(int64(len("key:0000"))+int64(len("value"))+128), store.PolicyAllKeysLRU)

	for i := 0; i < 500; i++ {
		if got := run(t, s, cmd("SET", fmt.Sprintf("key:%04d", i), "value")); got != "+OK\r\n" {
			t.Fatalf("write %d replied %q; eviction is not keeping up", i, got)
		}
	}

	// Eviction runs *before* each write, so the keyspace can sit one command
	// over the budget — the size of a write is not known until it happens. What
	// it must never do is overshoot by an unbounded amount.
	const oneEntry = int64(len("key:0000") + len("value") + 128)
	st := s.Store()
	if st.Used() > st.MaxMemory()+oneEntry {
		t.Errorf("used %d bytes against a %d byte budget (one entry of overshoot is expected)",
			st.Used(), st.MaxMemory())
	}
	if evicted := st.Stats().EvictedKeys.Load(); evicted == 0 {
		t.Error("500 writes into a 50-key budget evicted nothing")
	}
	// And the most recent writes are the ones that survived.
	if got := run(t, s, cmd("GET", "key:0499")); got != "$5\r\nvalue\r\n" {
		t.Errorf("the newest key is missing: %q", got)
	}
}

func TestDBSize(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	script := cmd("DBSIZE") +
		cmd("SET", "a", "1") + cmd("SET", "b", "2") + cmd("DBSIZE") +
		cmd("DEL", "a") + cmd("DBSIZE") +
		// An expired key is invisible to GET, so it must be invisible to
		// DBSIZE too — a count that disagrees with the other commands is worse
		// than no count.
		cmd("SET", "c", "v", "PX", "1") + cmd("DBSIZE")

	got := run(t, s, script)
	want := ":0\r\n" + "+OK\r\n+OK\r\n:2\r\n" + ":1\r\n:1\r\n" + "+OK\r\n:2\r\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestInfo(t *testing.T) {
	s := limited(t, 1<<20, store.PolicyAllKeysLRU)
	run(t, s, cmd("SET", "k", "v")+cmd("GET", "k")+cmd("GET", "missing"))

	reply := run(t, s, cmd("INFO"))
	for _, want := range []string{
		"# Memory", "used_memory:", "maxmemory:1048576", "maxmemory_policy:allkeys-lru",
		"# Stats", "keyspace_hits:1", "keyspace_misses:1", "evicted_keys:0", "expired_keys:0",
		"# Persistence", "aof_enabled:0", "# Keyspace", "db0:keys=1",
	} {
		if !strings.Contains(reply, want) {
			t.Errorf("INFO is missing %q:\n%s", want, reply)
		}
	}

	// A section argument narrows it, as in real Redis.
	section := run(t, s, cmd("INFO", "memory"))
	if !strings.Contains(section, "used_memory:") {
		t.Errorf("INFO memory has no memory section:\n%s", section)
	}
	if strings.Contains(section, "keyspace_hits") {
		t.Errorf("INFO memory returned other sections:\n%s", section)
	}
}
