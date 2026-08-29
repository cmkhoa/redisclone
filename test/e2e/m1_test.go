// M1 black-box tests: GET/SET/DEL/EXISTS over a real TCP connection, against
// the same binary the M0 suite boots (see m0_test.go for the harness).
//
// Same rule as M0: no RESP parser in here. Every check is an exact-bytes
// comparison or an "-ERR" prefix check.
//
// Every test uses key names derived from t.Name(), because all of these tests
// share one server process and, unlike M0's stateless PING/ECHO, they leave
// state behind. Two tests using "k" would pass alone and fail together.
package e2e

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// key returns a key name unique to the running test.
func key(t *testing.T, suffix string) string {
	t.Helper()
	return t.Name() + ":" + suffix
}

func TestSetGet(t *testing.T) {
	c := dial(t)
	k := key(t, "k")
	sendRecvExact(t, c, cmd("SET", k, "hello"), "+OK\r\n")
	sendRecvExact(t, c, cmd("GET", k), bulk("hello"))
}

func TestGetMissingKeyIsNullBulkString(t *testing.T) {
	c := dial(t)
	sendRecvExact(t, c, cmd("GET", key(t, "never-set")), "$-1\r\n")
}

// A key holding the empty string exists; a missing key does not. The two
// replies differ ($0 vs $-1) and clients depend on the difference.
func TestGetEmptyValueIsNotMissing(t *testing.T) {
	c := dial(t)
	k := key(t, "k")
	sendRecvExact(t, c, cmd("SET", k, ""), "+OK\r\n")
	sendRecvExact(t, c, cmd("GET", k), "$0\r\n\r\n")
	sendRecvExact(t, c, cmd("EXISTS", k), ":1\r\n")
}

func TestSetOverwrites(t *testing.T) {
	c := dial(t)
	k := key(t, "k")
	sendRecvExact(t, c, cmd("SET", k, "first"), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", k, "second"), "+OK\r\n")
	sendRecvExact(t, c, cmd("GET", k), bulk("second"))
}

func TestKeyspaceCommandsAreCaseInsensitive(t *testing.T) {
	c := dial(t)
	k := key(t, "k")
	sendRecvExact(t, c, cmd("set", k, "v"), "+OK\r\n")
	sendRecvExact(t, c, cmd("GeT", k), bulk("v"))
	sendRecvExact(t, c, cmd("eXiStS", k), ":1\r\n")
	sendRecvExact(t, c, cmd("dEl", k), ":1\r\n")
}

// Command names are case-insensitive; keys are not.
func TestKeysAreCaseSensitive(t *testing.T) {
	c := dial(t)
	lower, upper := key(t, "k"), key(t, "K")
	sendRecvExact(t, c, cmd("SET", lower, "lower"), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", upper, "upper"), "+OK\r\n")
	sendRecvExact(t, c, cmd("GET", lower), bulk("lower"))
	sendRecvExact(t, c, cmd("GET", upper), bulk("upper"))
}

func TestDel(t *testing.T) {
	c := dial(t)
	a, b := key(t, "a"), key(t, "b")
	sendRecvExact(t, c, cmd("SET", a, "1"), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", b, "2"), "+OK\r\n")
	// Two of the three keys exist.
	sendRecvExact(t, c, cmd("DEL", a, key(t, "missing"), b), ":2\r\n")
	sendRecvExact(t, c, cmd("GET", a), "$-1\r\n")
	sendRecvExact(t, c, cmd("EXISTS", a, b), ":0\r\n")
}

func TestDelMissingKeyIsZero(t *testing.T) {
	c := dial(t)
	sendRecvExact(t, c, cmd("DEL", key(t, "nope")), ":0\r\n")
}

// DEL counts keys removed, so a repeated key counts once; EXISTS answers per
// argument, so a repeated key counts every time.
func TestDelAndExistsCountDuplicatesDifferently(t *testing.T) {
	c := dial(t)
	k := key(t, "k")
	sendRecvExact(t, c, cmd("SET", k, "v"), "+OK\r\n")
	sendRecvExact(t, c, cmd("EXISTS", k, k, k), ":3\r\n")
	sendRecvExact(t, c, cmd("DEL", k, k), ":1\r\n")
}

// Keys and values are byte strings all the way down: RESP is length-prefixed,
// so CRLF and NUL inside either one must survive the round trip.
func TestKeysAndValuesAreBinarySafe(t *testing.T) {
	c := dial(t)
	k := key(t, "a\r\nb\x00c")
	v := "v\r\n\x00\xffx"
	sendRecvExact(t, c, cmd("SET", k, v), "+OK\r\n")
	sendRecvExact(t, c, cmd("GET", k), bulk(v))
	// A prefix of the binary key must not match it.
	sendRecvExact(t, c, cmd("GET", key(t, "a")), "$-1\r\n")
}

func TestLargeValueRoundTrips(t *testing.T) {
	c := dial(t)
	k := key(t, "big")
	v := strings.Repeat("y", 1<<20) // 1 MiB
	sendRecvExact(t, c, cmd("SET", k, v), "+OK\r\n")
	sendRecvExact(t, c, cmd("GET", k), bulk(v))
	sendRecvExact(t, c, cmd("DEL", k), ":1\r\n")
}

func TestKeyspaceArityErrors(t *testing.T) {
	c := dial(t)
	for _, req := range []string{
		cmd("SET"),
		cmd("SET", key(t, "k")),
		cmd("SET", key(t, "k"), "v", "EX", "10", "NX"), // NX is not implemented
		cmd("GET"),
		cmd("GET", "a", "b"),
		cmd("DEL"),
		cmd("EXISTS"),
	} {
		sendRecvError(t, c, req)
	}
	// The connection survives every one of them.
	sendRecvExact(t, c, cmd("PING"), "+PONG\r\n")
}

// The keyspace is shared: what one connection writes, another reads.
func TestKeyspaceIsSharedAcrossConnections(t *testing.T) {
	writer, reader := dial(t), dial(t)
	k := key(t, "shared")

	sendRecvExact(t, writer, cmd("SET", k, "written-elsewhere"), "+OK\r\n")
	sendRecvExact(t, reader, cmd("GET", k), bulk("written-elsewhere"))

	sendRecvExact(t, writer, cmd("DEL", k), ":1\r\n")
	sendRecvExact(t, reader, cmd("GET", k), "$-1\r\n")
}

// The M1 headline: many connections hammering the keyspace at once, correctly.
// Each client owns a private key range, so every reply is deterministic no
// matter how the goroutines interleave. Anything that returns another client's
// value, or a torn value, fails here.
func TestConcurrentClientsOwnKeys(t *testing.T) {
	const (
		clients    = 50
		iterations = 20
	)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.Dial("tcp", serverAddr)
			if err != nil {
				t.Errorf("client %d dial: %v", i, err)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(20 * time.Second))

			for j := 0; j < iterations; j++ {
				k := fmt.Sprintf("%s:client:%d:%d", t.Name(), i, j)
				v := fmt.Sprintf("value-%d-%d", i, j)

				// One pipelined batch per iteration: SET, GET, EXISTS, DEL,
				// GET. The whole reply is one exact byte string.
				req := cmd("SET", k, v) + cmd("GET", k) + cmd("EXISTS", k) +
					cmd("DEL", k) + cmd("GET", k)
				want := "+OK\r\n" + bulk(v) + ":1\r\n" + ":1\r\n" + "$-1\r\n"

				if _, err := c.Write([]byte(req)); err != nil {
					t.Errorf("client %d write: %v", i, err)
					return
				}
				buf := make([]byte, len(want))
				if _, err := io.ReadFull(c, buf); err != nil {
					t.Errorf("client %d read: %v", i, err)
					return
				}
				if string(buf) != want {
					t.Errorf("client %d iteration %d:\n got:  %q\n want: %q", i, j, buf, want)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// Many connections writing the *same* key concurrently. The final value must be
// one that somebody actually wrote — never a mixture of two — and every read
// along the way must return a whole value.
func TestConcurrentWritersToOneKey(t *testing.T) {
	const clients = 25
	k := key(t, "contended")

	valid := make(map[string]bool, clients)
	for i := 0; i < clients; i++ {
		valid[fmt.Sprintf("writer-%02d", i)] = true
	}

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.Dial("tcp", serverAddr)
			if err != nil {
				t.Errorf("client %d dial: %v", i, err)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(20 * time.Second))

			v := fmt.Sprintf("writer-%02d", i)
			for j := 0; j < 20; j++ {
				if _, err := c.Write([]byte(cmd("SET", k, v))); err != nil {
					t.Errorf("client %d write: %v", i, err)
					return
				}
				buf := make([]byte, len("+OK\r\n"))
				if _, err := io.ReadFull(c, buf); err != nil {
					t.Errorf("client %d read: %v", i, err)
					return
				}
				if string(buf) != "+OK\r\n" {
					t.Errorf("client %d: SET replied %q", i, buf)
					return
				}

				// Read it back. All writers use the same 9-byte value length,
				// so the reply length is known without parsing.
				if _, err := c.Write([]byte(cmd("GET", k))); err != nil {
					t.Errorf("client %d write: %v", i, err)
					return
				}
				got := make([]byte, len(bulk(v)))
				if _, err := io.ReadFull(c, got); err != nil {
					t.Errorf("client %d read: %v", i, err)
					return
				}
				const prefix = "$9\r\n"
				s := string(got)
				if !strings.HasPrefix(s, prefix) || !strings.HasSuffix(s, "\r\n") {
					t.Errorf("client %d: malformed reply %q", i, got)
					return
				}
				if v := s[len(prefix) : len(s)-2]; !valid[v] {
					t.Errorf("client %d: contended key read back as %q, which nobody wrote", i, v)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
