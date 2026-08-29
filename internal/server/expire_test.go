package server

import (
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

// The command-layer half of expiry: option parsing, the three TTL replies, and
// the error messages. What actually expires when is the store's business and is
// tested there, deterministically; these tests only sleep where the point is
// that the two layers are wired together.

func TestExpiryCommands(t *testing.T) {
	tests := []struct {
		name string
		req  string
		want string
	}{
		{
			"set with EX sets a deadline",
			cmd("SET", "k", "v", "EX", "100") + cmd("TTL", "k"),
			"+OK\r\n:100\r\n",
		},
		{
			"set with PX takes milliseconds",
			cmd("SET", "k", "v", "PX", "100000") + cmd("TTL", "k"),
			"+OK\r\n:100\r\n",
		},
		{
			"option name is case-insensitive",
			cmd("SET", "k", "v", "ex", "100") + cmd("TTL", "k"),
			"+OK\r\n:100\r\n",
		},
		{
			"ttl on a persistent key is -1",
			cmd("SET", "k", "v") + cmd("TTL", "k"),
			"+OK\r\n:-1\r\n",
		},
		{
			// -2 and -1 are the difference between "cache miss" and "cached
			// forever", so the two sentinels must not be confused.
			"ttl on a missing key is -2",
			cmd("TTL", "nope"),
			":-2\r\n",
		},
		{
			"a plain set clears an existing ttl",
			cmd("SET", "k", "v", "EX", "100") + cmd("SET", "k", "v2") + cmd("TTL", "k"),
			"+OK\r\n+OK\r\n:-1\r\n",
		},
		{
			"expire on an existing key returns 1",
			cmd("SET", "k", "v") + cmd("EXPIRE", "k", "100") + cmd("TTL", "k"),
			"+OK\r\n:1\r\n:100\r\n",
		},
		{
			"expire on a missing key returns 0",
			cmd("EXPIRE", "nope", "100"),
			":0\r\n",
		},
		{
			"expire replaces an existing deadline",
			cmd("SET", "k", "v", "EX", "10") + cmd("EXPIRE", "k", "100") + cmd("TTL", "k"),
			"+OK\r\n:1\r\n:100\r\n",
		},
		{
			// A deadline in the past is a documented way to delete a key.
			"expire with a past deadline deletes the key",
			cmd("SET", "k", "v") + cmd("EXPIRE", "k", "-1") + cmd("GET", "k") + cmd("TTL", "k"),
			"+OK\r\n:1\r\n$-1\r\n:-2\r\n",
		},
		{
			"pexpire takes milliseconds",
			cmd("SET", "k", "v") + cmd("PEXPIRE", "k", "100000") + cmd("TTL", "k"),
			"+OK\r\n:1\r\n:100\r\n",
		},
		{
			"pttl reports milliseconds",
			cmd("SET", "k", "v", "EX", "100") + cmd("PTTL", "k"),
			"+OK\r\n:100000\r\n",
		},
		{
			"pttl sentinels match ttl's",
			cmd("SET", "k", "v") + cmd("PTTL", "k") + cmd("PTTL", "nope"),
			"+OK\r\n:-1\r\n:-2\r\n",
		},
		{
			// TTL rounds to nearest, so a key set with EX 100 reads back as
			// 100 rather than 99 despite the microseconds spent getting here.
			"ttl rounds to the nearest second",
			cmd("SET", "k", "v", "PX", "1600") + cmd("TTL", "k"),
			"+OK\r\n:2\r\n",
		},
		{
			// ...and a key with 10ms left reports 1, never 0: a client reading
			// 0 would take it as "expiring this instant".
			"a live key never reports a ttl of zero",
			cmd("SET", "k", "v", "PX", "10") + cmd("TTL", "k"),
			"+OK\r\n:1\r\n",
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

func TestExpiryErrors(t *testing.T) {
	tests := []struct {
		name string
		req  string
		want string
	}{
		{"unknown set option", cmd("SET", "k", "v", "FOO", "10"), "-ERR syntax error\r\n"},
		{"set option without its value", cmd("SET", "k", "v", "EX"), "-ERR syntax error\r\n"},
		{
			"non-integer expiry",
			cmd("SET", "k", "v", "EX", "soon"),
			"-ERR value is not an integer or out of range\r\n",
		},
		{
			// A non-positive expiry on SET is almost always a caller bug, and
			// storing-then-deleting would hide it.
			"zero expiry on set",
			cmd("SET", "k", "v", "EX", "0"),
			"-ERR invalid expire time in 'set' command\r\n",
		},
		{
			"negative expiry on set",
			cmd("SET", "k", "v", "EX", "-1"),
			"-ERR invalid expire time in 'set' command\r\n",
		},
		{
			"non-integer ttl on expire",
			cmd("EXPIRE", "k", "later"),
			"-ERR value is not an integer or out of range\r\n",
		},
		{
			// Fits in an int64 as a count of seconds, overflows int64 as a
			// count of nanoseconds — which without the guard would wrap to a
			// deadline in the past and delete the key.
			"expiry that overflows a Duration",
			cmd("EXPIRE", "k", "10000000000"),
			"-ERR value is not an integer or out of range\r\n",
		},
		{
			"expiry larger than an int64",
			cmd("EXPIRE", "k", "99999999999999999999"),
			"-ERR value is not an integer or out of range\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exchange(t, tt.req+cmd("PING"))
			if got != tt.want+"+PONG\r\n" {
				t.Errorf("got %q, want %q", got, tt.want+"+PONG\r\n")
			}
		})
	}
}

func TestExpiryArityErrors(t *testing.T) {
	for _, req := range []string{
		cmd("EXPIRE", "k"), cmd("EXPIRE"), cmd("EXPIRE", "k", "1", "2"),
		cmd("TTL"), cmd("TTL", "a", "b"), cmd("PTTL"), cmd("PEXPIRE", "k"),
		cmd("SET", "k", "v", "EX", "10", "NX"),
	} {
		got := exchange(t, req+cmd("PING"))
		if !strings.HasPrefix(got, "-ERR wrong number of arguments") {
			t.Errorf("%q: got %q, want an arity error", req, got)
		}
	}
}

// The wiring test: a key with a short deadline really does become invisible,
// through the command layer, on a live connection.
func TestKeyBecomesInvisibleWhenItExpires(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	client, server := net.Pipe()
	defer client.Close()
	go s.HandleConn(server)

	if got := talk(t, client, cmd("SET", "k", "v", "PX", "30")); got != "+OK\r\n" {
		t.Fatalf("SET: %q", got)
	}
	if got := talk(t, client, cmd("GET", "k")); got != "$1\r\nv\r\n" {
		t.Fatalf("GET before the deadline: %q, want the value", got)
	}

	time.Sleep(80 * time.Millisecond)

	// Every read path has to agree that the key is gone.
	want := "$-1\r\n" + ":0\r\n" + ":-2\r\n" + ":0\r\n"
	got := talk(t, client, cmd("GET", "k")+cmd("EXISTS", "k")+cmd("TTL", "k")+cmd("DEL", "k"))
	if got != want {
		t.Errorf("after expiry: got %q, want %q", got, want)
	}

	// And the lazy path really deleted it rather than hiding it.
	if n := s.Store().Len(); n != 0 {
		t.Errorf("keyspace still holds %d keys", n)
	}
}
