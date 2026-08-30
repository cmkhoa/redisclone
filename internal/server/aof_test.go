package server

import (
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"redisclone/internal/aof"
)

// durable returns a server journalling to a fresh file, plus that file's path.
func durable(t *testing.T, policy aof.Policy) (*Server, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	s := New(log.New(io.Discard, "", 0))
	l, err := aof.Open(path, policy)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	s.AttachAOF(l)
	return s, path
}

// run executes a script of commands against s over a pipe and returns the
// replies.
func run(t *testing.T, s *Server, script string) string {
	t.Helper()
	client, server := net.Pipe()
	defer client.Close()
	go s.HandleConn(server)
	return talk(t, client, script)
}

// The property that matters: replaying the log rebuilds a keyspace that answers
// every question the same way the original did.
func TestReplayRebuildsTheKeyspace(t *testing.T) {
	original, path := durable(t, aof.PolicyEverysec)

	run(t, original, strings.Join([]string{
		cmd("SET", "plain", "value"),
		cmd("SET", "overwritten", "first"),
		cmd("SET", "overwritten", "second"),
		cmd("SET", "binary", "a\r\nb\x00c"),
		cmd("SET", "empty", ""),
		cmd("SET", "deleted", "gone"),
		cmd("DEL", "deleted"),
		cmd("SET", "volatile", "v", "EX", "1000"),
		cmd("SET", "expired-already", "v", "PX", "20"),
		cmd("SET", "given-a-ttl", "v"),
		cmd("EXPIRE", "given-a-ttl", "2000"),
		cmd("SET", "ttl-then-cleared", "v", "EX", "50"),
		cmd("SET", "ttl-then-cleared", "v2"),
		cmd("MSET", "batch-a", "1", "batch-b", "two"),
		cmd("INCR", "batch-a"),
		cmd("APPEND", "batch-b", "!"),
	}, ""))

	// Let the short-lived key actually expire before the restart.
	time.Sleep(60 * time.Millisecond)

	// The questions to ask both servers. Anything the log gets wrong shows up
	// as a different reply.
	questions := strings.Join([]string{
		cmd("GET", "plain"), cmd("GET", "overwritten"), cmd("GET", "binary"),
		cmd("GET", "empty"), cmd("EXISTS", "empty"), cmd("GET", "deleted"),
		cmd("GET", "expired-already"), cmd("TTL", "expired-already"),
		cmd("TTL", "plain"), cmd("TTL", "volatile"), cmd("TTL", "given-a-ttl"),
		cmd("GET", "ttl-then-cleared"), cmd("TTL", "ttl-then-cleared"),
		cmd("MGET", "batch-a", "batch-b"), cmd("STRLEN", "batch-b"),
	}, "")

	want := run(t, original, questions)

	// Flush and hand the file to a fresh server, as a restart would.
	if err := original.aof.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	restarted := New(log.New(io.Discard, "", 0))
	res, err := restarted.ReplayAOF(path)
	if err != nil {
		t.Fatalf("ReplayAOF: %v", err)
	}
	if res.Commands == 0 {
		t.Fatal("replayed nothing")
	}

	if got := run(t, restarted, questions); got != want {
		t.Errorf("the restarted server disagrees with the original:\n got:  %q\n want: %q", got, want)
	}
}

// A key whose deadline passed while the server was down must stay dead. This is
// the failure the absolute-deadline decision exists to prevent: a log saying
// "expires in 50ms" would hand it another 50ms on every restart, forever.
func TestExpiredKeysDoNotComeBackFromTheLog(t *testing.T) {
	original, path := durable(t, aof.PolicyEverysec)
	run(t, original, cmd("SET", "k", "v", "PX", "30")+cmd("SET", "survivor", "v", "EX", "500"))
	original.aof.Sync()

	time.Sleep(80 * time.Millisecond) // the "downtime"

	restarted := New(log.New(io.Discard, "", 0))
	if _, err := restarted.ReplayAOF(path); err != nil {
		t.Fatalf("ReplayAOF: %v", err)
	}

	if got := run(t, restarted, cmd("GET", "k")); got != "$-1\r\n" {
		t.Errorf("expired key came back as %q", got)
	}
	if got := run(t, restarted, cmd("GET", "survivor")); got != "$1\r\nv\r\n" {
		t.Errorf("a key that had not expired was lost: %q", got)
	}
	// It must be gone, not merely hidden: replaying a SET with a past deadline
	// should not leave an entry for the sampler to find later.
	if n := restarted.Store().Len(); n != 1 {
		t.Errorf("keyspace holds %d keys after replay, want 1", n)
	}
}

// The log is supposed to contain write commands this server wrote. Anything
// else means the file is not what we think it is.
func TestReplayRejectsCommandsThatDoNotBelong(t *testing.T) {
	tests := []struct {
		name string
		recs []string
		want string
	}{
		{"a read command", []string{cmd("GET", "k")}, "not a write command"},
		{"an unknown command", []string{cmd("FLYTOTHEMOON", "now")}, "unknown command"},
		{"wrong arity", []string{cmd("SET", "k")}, "wrong number of arguments"},
		{
			// A record the server would have refused from a client must not be
			// waved through at startup just because it is on disk.
			"a command the server would reject",
			[]string{cmd("SET", "k", "v", "EX", "0")},
			"invalid expire time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "appendonly.aof")
			if err := os.WriteFile(path, []byte(strings.Join(tt.recs, "")), 0o644); err != nil {
				t.Fatal(err)
			}
			s := New(log.New(io.Discard, "", 0))
			_, err := s.ReplayAOF(path)
			if err == nil {
				t.Fatalf("ReplayAOF accepted %v", tt.recs)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

// Under the "always" policy the promise is that nothing is acknowledged before
// it is on the disk. By the time the client has the +OK, the bytes are there.
func TestAlwaysPolicySyncsBeforeReplying(t *testing.T) {
	s, path := durable(t, aof.PolicyAlways)

	if got := run(t, s, cmd("SET", "k", "v")); got != "+OK\r\n" {
		t.Fatalf("SET replied %q", got)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() == 0 {
		t.Error("the client was told the write succeeded while the log was still empty")
	}
}

// Reads are unaffected by a broken log; writes are refused rather than accepted
// into memory and lost at the next restart.
func TestWritesAreRefusedWhenTheLogHasFailed(t *testing.T) {
	s, _ := durable(t, aof.PolicyEverysec)
	run(t, s, cmd("SET", "before", "v"))

	// Break the log the way a full disk would.
	s.aof.Close()
	s.aof.Append([]byte("SET"), []byte("k"), []byte(strings.Repeat("x", 128*1024)))
	s.aof.Sync()
	if s.aof.Err() == nil {
		t.Fatal("could not get the log into a failed state")
	}

	for _, c := range []string{
		cmd("SET", "k", "v"), cmd("DEL", "before"),
		cmd("EXPIRE", "before", "10"), cmd("PEXPIREAT", "before", "99999999999999"),
	} {
		if got := run(t, s, c); !strings.HasPrefix(got, "-MISCONF") {
			t.Errorf("%q replied %q, want -MISCONF", c, got)
		}
	}

	// A degraded cache still serves reads.
	if got := run(t, s, cmd("GET", "before")+cmd("PING")); got != "$1\r\nv\r\n+PONG\r\n" {
		t.Errorf("reads stopped working: %q", got)
	}
}

// Nothing is journalled when durability is off — including the fact that the
// server has to work at all with a nil log.
func TestNoJournalWhenDurabilityIsOff(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	if got := run(t, s, cmd("SET", "k", "v")+cmd("GET", "k")); got != "+OK\r\n$1\r\nv\r\n" {
		t.Errorf("got %q", got)
	}
}

// The absolute-time commands are real commands, not just log records.
func TestExpireAtCommands(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	future := time.Now().Add(time.Hour)

	script := cmd("SET", "k", "v") +
		cmd("PEXPIREAT", "k", itoa(future.UnixMilli())) +
		cmd("TTL", "k") +
		cmd("SET", "j", "v") +
		cmd("EXPIREAT", "j", itoa(future.Unix())) +
		cmd("TTL", "j") +
		// A deadline in the past deletes the key.
		cmd("SET", "old", "v") +
		cmd("PEXPIREAT", "old", "1") +
		cmd("GET", "old") +
		cmd("PEXPIREAT", "missing", itoa(future.UnixMilli()))

	want := "+OK\r\n:1\r\n:3600\r\n" + "+OK\r\n:1\r\n:3600\r\n" + "+OK\r\n:1\r\n$-1\r\n" + ":0\r\n"
	got := run(t, s, script)
	// The two commands are issued on either side of a wall-clock second
	// boundary often enough to make an exact 3600-second assertion flaky.
	// Both answers still prove the key has roughly an hour left.
	justUnder := "+OK\r\n:1\r\n:3600\r\n" + "+OK\r\n:1\r\n:3599\r\n" + "+OK\r\n:1\r\n$-1\r\n" + ":0\r\n"
	if got != want && got != justUnder {
		t.Errorf("got %q, want about an hour remaining", got)
	}
}

func TestBGRewriteAOFCompactsAndReplays(t *testing.T) {
	original, path := durable(t, aof.PolicyEverysec)
	for i := 0; i < 50; i++ {
		run(t, original, cmd("SET", "churn", strconv.Itoa(i)))
	}
	run(t, original, cmd("SET", "kept", "value", "EX", "600"))
	if got := run(t, original, cmd("BGREWRITEAOF")); got != "+Background append only file rewriting started\r\n" {
		t.Fatalf("BGREWRITEAOF = %q", got)
	}
	original.WaitBackground()
	if err := original.aof.Sync(); err != nil {
		t.Fatal(err)
	}

	restarted := New(log.New(io.Discard, "", 0))
	if _, err := restarted.ReplayAOF(path); err != nil {
		t.Fatal(err)
	}
	if got := run(t, restarted, cmd("GET", "churn")+cmd("GET", "kept")); got != "$2\r\n49\r\n$5\r\nvalue\r\n" {
		t.Errorf("replay after rewrite = %q", got)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
