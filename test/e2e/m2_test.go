// M2 black-box tests: EXPIRE/TTL and SET's expiry options over a real TCP
// connection, against the same binary the M0 suite boots.
//
// Same rule as the other suites: no RESP parser. Integer replies are read as a
// single line and compared or converted with strconv — reading one line is not
// parsing RESP, and it is what lets a test assert "the TTL is somewhere in this
// range" without pulling the server's own decoder into its own test.
package e2e

import (
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// sendRecvInt writes req and returns the integer reply.
func sendRecvInt(t *testing.T, c net.Conn, req string) int64 {
	t.Helper()
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatalf("write: %v", err)
	}
	line := readLine(t, c)
	if !strings.HasPrefix(line, ":") || !strings.HasSuffix(line, "\r\n") {
		t.Fatalf("expected an integer reply, got %q", line)
	}
	n, err := strconv.ParseInt(line[1:len(line)-2], 10, 64)
	if err != nil {
		t.Fatalf("integer reply %q: %v", line, err)
	}
	return n
}

func TestSetWithExpiry(t *testing.T) {
	c := dial(t)
	k := key(t, "k")

	sendRecvExact(t, c, cmd("SET", k, "v", "EX", "100"), "+OK\r\n")
	sendRecvExact(t, c, cmd("GET", k), bulk("v"))
	sendRecvExact(t, c, cmd("TTL", k), ":100\r\n")

	// PTTL sees the milliseconds actually ticking away.
	if ms := sendRecvInt(t, c, cmd("PTTL", k)); ms > 100_000 || ms < 99_000 {
		t.Errorf("PTTL = %d, want just under 100000", ms)
	}
}

func TestTTLSentinels(t *testing.T) {
	c := dial(t)
	k := key(t, "k")

	// -2 means "no such key", -1 means "no deadline". A client that confuses
	// them treats a cache miss as a permanent entry.
	sendRecvExact(t, c, cmd("TTL", k), ":-2\r\n")
	sendRecvExact(t, c, cmd("SET", k, "v"), "+OK\r\n")
	sendRecvExact(t, c, cmd("TTL", k), ":-1\r\n")
	sendRecvExact(t, c, cmd("PTTL", k), ":-1\r\n")
}

func TestExpireAndPersistBehaviour(t *testing.T) {
	c := dial(t)
	k := key(t, "k")

	sendRecvExact(t, c, cmd("EXPIRE", k, "100"), ":0\r\n") // no such key yet
	sendRecvExact(t, c, cmd("SET", k, "v"), "+OK\r\n")
	sendRecvExact(t, c, cmd("EXPIRE", k, "100"), ":1\r\n")
	sendRecvExact(t, c, cmd("TTL", k), ":100\r\n")

	// A plain SET makes the key permanent again.
	sendRecvExact(t, c, cmd("SET", k, "v2"), "+OK\r\n")
	sendRecvExact(t, c, cmd("TTL", k), ":-1\r\n")
}

// EXPIRE with a deadline in the past is a documented way to delete a key.
func TestExpireInThePastDeletes(t *testing.T) {
	c := dial(t)
	k := key(t, "k")
	sendRecvExact(t, c, cmd("SET", k, "v"), "+OK\r\n")
	sendRecvExact(t, c, cmd("EXPIRE", k, "-1"), ":1\r\n")
	sendRecvExact(t, c, cmd("GET", k), "$-1\r\n")
	sendRecvExact(t, c, cmd("EXISTS", k), ":0\r\n")
}

// The headline: a key really does disappear when its deadline passes, on every
// read path, over a real connection.
func TestKeyExpiresForReal(t *testing.T) {
	c := dial(t)
	k := key(t, "k")

	sendRecvExact(t, c, cmd("SET", k, "v", "PX", "50"), "+OK\r\n")
	sendRecvExact(t, c, cmd("GET", k), bulk("v"))

	time.Sleep(150 * time.Millisecond)

	sendRecvExact(t, c, cmd("GET", k), "$-1\r\n")
	sendRecvExact(t, c, cmd("EXISTS", k), ":0\r\n")
	sendRecvExact(t, c, cmd("TTL", k), ":-2\r\n")
	// DEL on a key that expired reports 0: it was already gone.
	sendRecvExact(t, c, cmd("DEL", k), ":0\r\n")
	// And EXPIRE cannot bring it back.
	sendRecvExact(t, c, cmd("EXPIRE", k, "100"), ":0\r\n")
}

// PEXPIRE on an existing key, then watch it go.
func TestPexpireExpiresTheKey(t *testing.T) {
	c := dial(t)
	k := key(t, "k")

	sendRecvExact(t, c, cmd("SET", k, "v"), "+OK\r\n")
	sendRecvExact(t, c, cmd("PEXPIRE", k, "50"), ":1\r\n")
	sendRecvExact(t, c, cmd("GET", k), bulk("v"))

	time.Sleep(150 * time.Millisecond)
	sendRecvExact(t, c, cmd("GET", k), "$-1\r\n")
}

// An expired key is invisible to *other* connections too, not just the one that
// wrote it — the deadline lives in the keyspace, not in a session.
func TestExpiryIsVisibleToOtherConnections(t *testing.T) {
	writer, reader := dial(t), dial(t)
	k := key(t, "k")

	sendRecvExact(t, writer, cmd("SET", k, "v", "PX", "50"), "+OK\r\n")
	sendRecvExact(t, reader, cmd("GET", k), bulk("v"))

	time.Sleep(150 * time.Millisecond)
	sendRecvExact(t, reader, cmd("GET", k), "$-1\r\n")
}

func TestExpiryErrors(t *testing.T) {
	c := dial(t)
	k := key(t, "k")
	for _, req := range []string{
		cmd("SET", k, "v", "FOO", "10"),        // unknown option
		cmd("SET", k, "v", "EX", "notanumber"), // unparseable
		cmd("SET", k, "v", "EX", "0"),          // non-positive expiry on SET
		cmd("SET", k, "v", "EX", "-5"),
		cmd("EXPIRE", k, "notanumber"),
		cmd("EXPIRE", k, "10000000000"),          // overflows a time.Duration
		cmd("EXPIRE", k, "99999999999999999999"), // overflows an int64
		cmd("TTL"),
		cmd("EXPIRE", k),
	} {
		sendRecvError(t, c, req)
	}
	// None of that disturbed the connection, and none of it created the key.
	sendRecvExact(t, c, cmd("EXISTS", k), ":0\r\n")
	sendRecvExact(t, c, cmd("PING"), "+PONG\r\n")
}

// A rejected SET must not have written anything — the error comes before the
// store is touched, not after.
func TestRejectedSetLeavesTheOldValue(t *testing.T) {
	c := dial(t)
	k := key(t, "k")
	sendRecvExact(t, c, cmd("SET", k, "original"), "+OK\r\n")
	sendRecvError(t, c, cmd("SET", k, "replacement", "EX", "0"))
	sendRecvExact(t, c, cmd("GET", k), bulk("original"))
	sendRecvExact(t, c, cmd("TTL", k), ":-1\r\n")
}
