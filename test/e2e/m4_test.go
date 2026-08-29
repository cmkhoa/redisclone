// M4 black-box tests: the memory budget and eviction, observed the way an
// operator would — through INFO and DBSIZE over a real connection.
//
// These start their own server processes, because the budget is a startup flag.
package e2e

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// startWith boots a server with the given extra flags and returns its address.
func startWith(t *testing.T, args ...string) string {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort())
	full := append([]string{"-addr", addr}, args...)
	cmd := exec.Command(serverBin, full...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	if !waitForPort(addr, 5*time.Second) {
		t.Fatalf("server never opened %s", addr)
	}
	return addr
}

func dialAddr(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(20 * time.Second))
	return c
}

// infoField reads INFO and pulls out one "field:value" line. The reply is a
// bulk string, so its length prefix says exactly how many bytes to read — no
// parser needed, just arithmetic.
func infoField(t *testing.T, c net.Conn, field string) string {
	t.Helper()
	if _, err := c.Write([]byte(cmd("INFO"))); err != nil {
		t.Fatalf("write: %v", err)
	}
	header := readLine(t, c) // "$<n>\r\n"
	if !strings.HasPrefix(header, "$") {
		t.Fatalf("INFO replied %q, want a bulk string", header)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(header[1:], "\r\n"))
	if err != nil {
		t.Fatalf("bad bulk header %q", header)
	}
	body := make([]byte, n+2) // payload plus its trailing CRLF
	if _, err := readFull(c, body); err != nil {
		t.Fatalf("read INFO body: %v", err)
	}
	for _, line := range strings.Split(string(body[:n]), "\r\n") {
		// The keyspace section follows Redis's `db0:keys=...` convention,
		// where the requested metric itself contains a colon rather than using
		// the ordinary `field:value` shape used by the other sections.
		if field == "db0:keys" && strings.HasPrefix(line, field+"=") {
			return strings.TrimPrefix(line, field+"=")
		}
		if name, value, ok := strings.Cut(line, ":"); ok && name == field {
			return value
		}
	}
	t.Fatalf("INFO has no field %q in:\n%s", field, body[:n])
	return ""
}

func infoInt(t *testing.T, c net.Conn, field string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(infoField(t, c, field), 10, 64)
	if err != nil {
		t.Fatalf("INFO %s is not an integer: %v", field, err)
	}
	return n
}

func readFull(c net.Conn, buf []byte) (int, error) {
	read := 0
	for read < len(buf) {
		n, err := c.Read(buf[read:])
		read += n
		if err != nil {
			return read, err
		}
	}
	return read, nil
}

// Without a budget nothing is ever evicted, however much is written.
func TestUnlimitedByDefault(t *testing.T) {
	c := dialAddr(t, startWith(t))
	for i := 0; i < 500; i++ {
		sendRecvExact(t, c, cmd("SET", fmt.Sprintf("k:%d", i), "value"), "+OK\r\n")
	}
	sendRecvExact(t, c, cmd("DBSIZE"), ":500\r\n")
	if got := infoInt(t, c, "evicted_keys"); got != 0 {
		t.Errorf("evicted_keys = %d with no maxmemory set", got)
	}
	if got := infoField(t, c, "maxmemory"); got != "0" {
		t.Errorf("maxmemory = %s, want 0", got)
	}
}

// The default policy protects data: when the budget runs out, writes are
// refused rather than keys silently disappearing.
func TestNoEvictionRepliesOOM(t *testing.T) {
	c := dialAddr(t, startWith(t, "-maxmemory", "8kb"))

	// Fill past the budget. The first writes succeed; at some point one is
	// refused, and every write after it is too.
	var refusedAt int
	for i := 0; i < 500; i++ {
		if _, err := c.Write([]byte(cmd("SET", fmt.Sprintf("k:%04d", i), strings.Repeat("v", 64)))); err != nil {
			t.Fatalf("write: %v", err)
		}
		line := readLine(t, c)
		if strings.HasPrefix(line, "-OOM") {
			refusedAt = i
			break
		}
		if line != "+OK\r\n" {
			t.Fatalf("write %d replied %q", i, line)
		}
	}
	if refusedAt == 0 {
		t.Fatal("never hit the memory budget")
	}

	// Reads still work, and nothing was thrown away.
	sendRecvExact(t, c, cmd("GET", "k:0000"), bulk(strings.Repeat("v", 64)))
	if got := infoInt(t, c, "evicted_keys"); got != 0 {
		t.Errorf("evicted_keys = %d under noeviction", got)
	}
	if got := int(infoInt(t, c, "db0:keys")); got != refusedAt {
		t.Errorf("db0:keys = %d, want the %d keys that were accepted", got, refusedAt)
	}
}

// With allkeys-lru the writes keep succeeding and the keyspace stops growing.
func TestLRUEvictionKeepsTheKeyspaceBounded(t *testing.T) {
	c := dialAddr(t, startWith(t, "-maxmemory", "16kb", "-maxmemory-policy", "allkeys-lru"))

	for i := 0; i < 2000; i++ {
		sendRecvExact(t, c, cmd("SET", fmt.Sprintf("k:%04d", i), strings.Repeat("v", 64)), "+OK\r\n")
	}

	used := infoInt(t, c, "used_memory")
	max := infoInt(t, c, "maxmemory")
	if used > max+512 { // one command's overshoot
		t.Errorf("used_memory = %d against maxmemory = %d", used, max)
	}
	if evicted := infoInt(t, c, "evicted_keys"); evicted == 0 {
		t.Error("2000 writes into a 16kb budget evicted nothing")
	}
	// The most recent write is the one that must have survived.
	sendRecvExact(t, c, cmd("GET", "k:1999"), bulk(strings.Repeat("v", 64)))
}

// volatile-lru evicts only what was marked disposable, and refuses the write
// when nothing is.
func TestVolatileLRUProtectsKeysWithoutDeadlines(t *testing.T) {
	c := dialAddr(t, startWith(t, "-maxmemory", "12kb", "-maxmemory-policy", "volatile-lru"))

	// Permanent data first, then cache entries that may be dropped.
	for i := 0; i < 20; i++ {
		sendRecvExact(t, c, cmd("SET", fmt.Sprintf("keep:%02d", i), strings.Repeat("v", 64)), "+OK\r\n")
	}
	var refused bool
	for i := 0; i < 500; i++ {
		if _, err := c.Write([]byte(cmd("SET", fmt.Sprintf("cache:%04d", i), strings.Repeat("v", 64), "EX", "600"))); err != nil {
			t.Fatal(err)
		}
		if line := readLine(t, c); strings.HasPrefix(line, "-OOM") {
			refused = true
			break
		}
	}

	// Every permanent key must still be there, whether or not we ran out.
	for i := 0; i < 20; i++ {
		sendRecvExact(t, c, cmd("EXISTS", fmt.Sprintf("keep:%02d", i)), ":1\r\n")
	}
	if evicted := infoInt(t, c, "evicted_keys"); evicted == 0 && !refused {
		t.Error("neither evicted nor refused anything under a 12kb budget")
	}
}

// INFO's counters are the only window onto expiration actually collecting —
// the gap M2 left open.
func TestInfoReportsExpiredKeys(t *testing.T) {
	c := dialAddr(t, startWith(t))
	for i := 0; i < 50; i++ {
		sendRecvExact(t, c, cmd("SET", fmt.Sprintf("k:%d", i), "v", "PX", "50"), "+OK\r\n")
	}

	// Nobody reads these keys, so only the background sampler can collect them.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if infoInt(t, c, "expired_keys") == 50 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := infoInt(t, c, "expired_keys"); got != 50 {
		t.Errorf("expired_keys = %d after 5s, want 50 — the sampler is not collecting", got)
	}
	sendRecvExact(t, c, cmd("DBSIZE"), ":0\r\n")
}

// Eviction is a decision this process made; nothing in the log implies it, so
// the DEL has to be written or a restart would bring every evicted key back.
func TestEvictionsSurviveIntoTheLog(t *testing.T) {
	dir := t.TempDir()
	addr := startWith(t, "-maxmemory", "16kb", "-maxmemory-policy", "allkeys-lru",
		"-appendonly", "-appendfsync", "always", "-dir", dir)
	c := dialAddr(t, addr)

	for i := 0; i < 1000; i++ {
		sendRecvExact(t, c, cmd("SET", fmt.Sprintf("k:%04d", i), strings.Repeat("v", 64)), "+OK\r\n")
	}
	before := infoInt(t, c, "db0:keys")
	if evicted := infoInt(t, c, "evicted_keys"); evicted == 0 {
		t.Fatal("nothing was evicted; the test proves nothing")
	}

	// Replay the log into a second server with no budget at all: if evictions
	// were not journalled, every evicted key would reappear.
	replayAddr := startWith(t, "-appendonly", "-dir", dir)
	c2 := dialAddr(t, replayAddr)
	if after := infoInt(t, c2, "db0:keys"); after != before {
		t.Errorf("replay produced %d keys, the running server had %d — evictions were not logged",
			after, before)
	}
}
