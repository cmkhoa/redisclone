package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// Keyspace commands, driven over net.Pipe like the M0 dispatch tests. Each
// case is a pipelined script of commands and the exact bytes the server must
// reply with — a script rather than one command per exchange, because the
// interesting behaviour is what GET says after SET.

func TestKeyspaceCommands(t *testing.T) {
	tests := []struct {
		name string
		req  string
		want string
	}{
		{
			"set then get",
			cmd("SET", "k", "v") + cmd("GET", "k"),
			"+OK\r\n$1\r\nv\r\n",
		},
		{
			"get on a missing key is the null bulk string",
			cmd("GET", "nope"),
			"$-1\r\n",
		},
		{
			// $0 vs $-1: a key holding "" exists, and clients tell the two apart.
			"empty value is not a missing key",
			cmd("SET", "k", "") + cmd("GET", "k") + cmd("EXISTS", "k"),
			"+OK\r\n$0\r\n\r\n:1\r\n",
		},
		{
			"set overwrites",
			cmd("SET", "k", "first") + cmd("SET", "k", "second") + cmd("GET", "k"),
			"+OK\r\n+OK\r\n$6\r\nsecond\r\n",
		},
		{
			"commands are case-insensitive",
			cmd("set", "k", "v") + cmd("gEt", "k"),
			"+OK\r\n$1\r\nv\r\n",
		},
		{
			// Keys are case-sensitive, unlike command names.
			"keys are case-sensitive",
			cmd("SET", "k", "lower") + cmd("SET", "K", "upper") + cmd("GET", "k") + cmd("GET", "K"),
			"+OK\r\n+OK\r\n$5\r\nlower\r\n$5\r\nupper\r\n",
		},
		{
			"del returns the number of keys removed",
			cmd("SET", "a", "1") + cmd("SET", "b", "2") +
				cmd("DEL", "a", "missing", "b") + cmd("EXISTS", "a", "b"),
			"+OK\r\n+OK\r\n:2\r\n:0\r\n",
		},
		{
			"del on a missing key is zero, not an error",
			cmd("DEL", "nope"),
			":0\r\n",
		},
		{
			"del of a repeated key counts once",
			cmd("SET", "a", "1") + cmd("DEL", "a", "a"),
			"+OK\r\n:1\r\n",
		},
		{
			// EXISTS counts duplicates; DEL cannot. Real Redis behaves this way
			// and it looks like a bug until you see the reasoning: EXISTS
			// answers per argument, DEL reports keys actually removed.
			"exists counts duplicates",
			cmd("SET", "a", "1") + cmd("EXISTS", "a", "a", "a"),
			"+OK\r\n:3\r\n",
		},
		{
			"exists on missing keys",
			cmd("EXISTS", "x", "y"),
			":0\r\n",
		},
		{
			"values are binary safe",
			cmd("SET", "k", "a\r\nb\x00c") + cmd("GET", "k"),
			"+OK\r\n$6\r\na\r\nb\x00c\r\n",
		},
		{
			"keys are binary safe",
			cmd("SET", "a\r\nb", "v") + cmd("GET", "a\r\nb") + cmd("GET", "a"),
			"+OK\r\n$1\r\nv\r\n$-1\r\n",
		},
		{
			"deleted key reads back as missing",
			cmd("SET", "k", "v") + cmd("DEL", "k") + cmd("GET", "k"),
			"+OK\r\n:1\r\n$-1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exchange(t, tt.req); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestKeyspaceArityErrors(t *testing.T) {
	tests := []struct {
		name string
		req  string
	}{
		{"set without value", cmd("SET", "k")},
		{"set with no arguments", cmd("SET")},
		// EX/PX landed in M2; NX, XX and KEEPTTL have not, and an option this
		// server does not implement must be refused rather than ignored — a
		// client that asks for NX and gets an unconditional overwrite is worse
		// off than one that gets -ERR. (Past four arguments that shows up as an
		// arity error; the syntax errors are in expire_test.go.)
		{"set with more options than are supported", cmd("SET", "k", "v", "EX", "10", "NX")},
		{"get with no key", cmd("GET")},
		{"get with two keys", cmd("GET", "a", "b")},
		{"del with no keys", cmd("DEL")},
		{"exists with no keys", cmd("EXISTS")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exchange(t, tt.req+cmd("PING"))
			if !strings.HasPrefix(got, "-ERR wrong number of arguments for '") {
				t.Fatalf("got %q, want a wrong-number-of-arguments error", got)
			}
			// The connection has to survive: an arity error is a reply.
			if !strings.HasSuffix(got, "+PONG\r\n") {
				t.Errorf("connection did not survive the arity error: %q", got)
			}
		})
	}
}

// The point of M1: the keyspace is shared across connections, and correct when
// many of them hit it at once. Run with -race.
func TestConcurrentClientsShareTheKeyspace(t *testing.T) {
	const (
		clients    = 16
		iterations = 50
	)
	s := New(log.New(io.Discard, "", 0))

	// One client writes a key, all the others must be able to read it.
	seed, seedSrv := net.Pipe()
	go s.HandleConn(seedSrv)
	if got := talk(t, seed, cmd("SET", "shared", "seeded")); got != "+OK\r\n" {
		t.Fatalf("seeding: got %q", got)
	}

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			client, server := net.Pipe()
			defer client.Close()
			go s.HandleConn(server)

			for i := 0; i < iterations; i++ {
				// A key only this client touches: fully deterministic even
				// while fifteen other goroutines hammer the same map.
				own := fmt.Sprintf("client:%d:%d", c, i)
				val := fmt.Sprintf("v%d", i)

				script := cmd("SET", own, val) + cmd("GET", own) + cmd("GET", "shared") + cmd("DEL", own)
				want := fmt.Sprintf("+OK\r\n$%d\r\n%s\r\n$6\r\nseeded\r\n:1\r\n", len(val), val)
				if got := talk(t, client, script); got != want {
					t.Errorf("client %d iteration %d: got %q, want %q", c, i, got, want)
					return
				}
			}
		}(c)
	}
	wg.Wait()

	// Only the seeded key should be left behind.
	if got := s.Store().Len(); got != 1 {
		t.Errorf("keyspace holds %d keys after every client cleaned up, want 1", got)
	}
}

// talk writes req on an already-open connection and reads exactly one reply
// batch back: everything the server sends before it goes quiet again.
func talk(t *testing.T, c net.Conn, req string) string {
	t.Helper()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatalf("write: %v", err)
	}
	// net.Pipe is unbuffered and synchronous, so one Read returns whatever the
	// server flushed — which, thanks to the flush-before-read hook, is the
	// whole pipelined batch of replies in one write.
	buf := make([]byte, 64*1024)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(buf[:n])
}

// cmd encodes args as a RESP array of bulk strings, the way a real client sends
// commands. (Deliberately duplicated from the e2e harness: these two suites do
// not share code, so a bug in one encoder cannot hide a bug in the other.)
func cmd(args ...string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	return sb.String()
}
