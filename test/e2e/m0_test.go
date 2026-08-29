// Package e2e is a black-box test harness for M0 (RESP + PING/ECHO).
//
// It builds ./cmd/redisclone, starts it on a free port, and speaks raw RESP
// bytes over TCP. It deliberately contains NO RESP parser — every check is
// either an exact-bytes comparison or a "reply starts with -ERR" check — so
// reading this file won't hand you the implementation.
//
// Contract this harness assumes of your server:
//   - `go build ./cmd/redisclone` produces the server binary
//   - the binary accepts `-addr <host:port>` (default ":6379")
//   - it is ready to accept connections shortly after the port opens
//
// Run with: make e2e
package e2e

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var serverAddr string

// serverBin is the binary TestMain built, so suites that need their own server
// process (M3 restarts one) do not each rebuild it.
var serverBin string

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "redisclone-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mkdtemp:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmp)

	bin := filepath.Join(tmp, "redisclone")
	build := exec.Command("go", "build", "-o", bin, "redisclone/cmd/redisclone")
	build.Dir = repoRoot()
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s", err, out)
		os.Exit(1)
	}

	serverBin = bin

	port := freePort()
	serverAddr = fmt.Sprintf("127.0.0.1:%d", port)

	srv := exec.Command(bin, "-addr", serverAddr)
	srv.Stdout = os.Stderr
	srv.Stderr = os.Stderr
	if err := srv.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start server:", err)
		os.Exit(1)
	}

	if !waitForPort(serverAddr, 3*time.Second) {
		srv.Process.Kill()
		fmt.Fprintln(os.Stderr, "server never opened", serverAddr)
		os.Exit(1)
	}

	code := m.Run()
	srv.Process.Kill()
	srv.Wait()
	os.Exit(code)
}

func repoRoot() string {
	wd, _ := os.Getwd()
	return filepath.Dir(filepath.Dir(wd)) // test/e2e -> repo root
}

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForPort(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// --- helpers -------------------------------------------------------------

func dial(t *testing.T) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("dial %s: %v", serverAddr, err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(5 * time.Second))
	return c
}

// sendRecvExact writes req and asserts the next len(want) bytes are exactly want.
func sendRecvExact(t *testing.T, c net.Conn, req, want string) {
	t.Helper()
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatalf("write: %v", err)
	}
	expectExact(t, c, want)
}

func expectExact(t *testing.T, c net.Conn, want string) {
	t.Helper()
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read (wanted %q): %v", want, err)
	}
	if string(buf) != want {
		t.Fatalf("reply mismatch:\n got:  %q\n want: %q", buf, want)
	}
}

// sendRecvError writes req and asserts the reply is one CRLF-terminated line
// starting with "-ERR".
func sendRecvError(t *testing.T, c net.Conn, req string) {
	t.Helper()
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatalf("write: %v", err)
	}
	line := readLine(t, c)
	if !strings.HasPrefix(line, "-ERR") {
		t.Fatalf("expected -ERR reply, got %q", line)
	}
}

// readLine reads bytes one at a time until \n (crude on purpose).
func readLine(t *testing.T, c net.Conn) string {
	t.Helper()
	var sb strings.Builder
	b := make([]byte, 1)
	for {
		if _, err := io.ReadFull(c, b); err != nil {
			t.Fatalf("read line: %v (so far %q)", err, sb.String())
		}
		sb.WriteByte(b[0])
		if b[0] == '\n' {
			return sb.String()
		}
	}
}

// bulk encodes s as a RESP bulk string.
func bulk(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// cmd encodes args as a RESP array of bulk strings (how real clients send commands).
func cmd(args ...string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		sb.WriteString(bulk(a))
	}
	return sb.String()
}

// --- tests ---------------------------------------------------------------

func TestPing(t *testing.T) {
	c := dial(t)
	sendRecvExact(t, c, cmd("PING"), "+PONG\r\n")
}

func TestPingIsCaseInsensitive(t *testing.T) {
	c := dial(t)
	sendRecvExact(t, c, cmd("ping"), "+PONG\r\n")
	sendRecvExact(t, c, cmd("PiNg"), "+PONG\r\n")
}

func TestPingWithArgument(t *testing.T) {
	// PING with an argument echoes it back as a bulk string.
	c := dial(t)
	sendRecvExact(t, c, cmd("PING", "hello"), bulk("hello"))
}

func TestEcho(t *testing.T) {
	c := dial(t)
	sendRecvExact(t, c, cmd("ECHO", "hello world"), bulk("hello world"))
}

func TestEchoEmptyString(t *testing.T) {
	c := dial(t)
	sendRecvExact(t, c, cmd("ECHO", ""), "$0\r\n\r\n")
}

func TestEchoIsBinarySafe(t *testing.T) {
	// Payload contains CRLF and NUL bytes; a line-based payload reader breaks here.
	payload := "a\r\nb\x00c\rd\ne"
	c := dial(t)
	sendRecvExact(t, c, cmd("ECHO", payload), bulk(payload))
}

func TestEchoLargePayload(t *testing.T) {
	payload := strings.Repeat("x", 1<<20) // 1 MiB
	c := dial(t)
	sendRecvExact(t, c, cmd("ECHO", payload), bulk(payload))
}

func TestEchoWrongArity(t *testing.T) {
	c := dial(t)
	sendRecvError(t, c, cmd("ECHO"))
	// Connection must survive an arity error.
	sendRecvExact(t, c, cmd("PING"), "+PONG\r\n")
}

func TestUnknownCommand(t *testing.T) {
	c := dial(t)
	sendRecvError(t, c, cmd("FLYTOTHEMOON", "now"))
	sendRecvExact(t, c, cmd("PING"), "+PONG\r\n")
}

func TestPipelining(t *testing.T) {
	// Two commands in a single write must yield two replies, in order.
	c := dial(t)
	sendRecvExact(t, c, cmd("PING")+cmd("ECHO", "abc"), "+PONG\r\n"+bulk("abc"))
}

func TestFragmentedWrites(t *testing.T) {
	// A command trickling in byte-by-byte must still parse: TCP is a byte
	// stream and gives no message boundaries.
	c := dial(t)
	req := cmd("ECHO", "frag")
	for i := 0; i < len(req); i++ {
		if _, err := c.Write([]byte{req[i]}); err != nil {
			t.Fatalf("write byte %d: %v", i, err)
		}
		time.Sleep(time.Millisecond)
	}
	expectExact(t, c, bulk("frag"))
}

func TestConcurrentClients(t *testing.T) {
	const clients = 50
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
			c.SetDeadline(time.Now().Add(10 * time.Second))
			msg := fmt.Sprintf("client-%d", i)
			for j := 0; j < 20; j++ {
				if _, err := c.Write([]byte(cmd("ECHO", msg))); err != nil {
					t.Errorf("client %d write: %v", i, err)
					return
				}
				want := bulk(msg)
				buf := make([]byte, len(want))
				if _, err := io.ReadFull(c, buf); err != nil {
					t.Errorf("client %d read: %v", i, err)
					return
				}
				if string(buf) != want {
					t.Errorf("client %d got %q want %q", i, buf, want)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestMalformedInputGetsErrorNotHang(t *testing.T) {
	// Garbage that isn't RESP. The server may reply -ERR and/or close the
	// connection — it must not hang or crash. (Real Redis treats input not
	// starting with '*' as an "inline command"; implementing that is optional,
	// so this only checks the server stays alive for other clients.)
	c := dial(t)
	c.Write([]byte("this is not resp\r\n"))
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 512)
	c.Read(buf) // whatever happens here is fine

	// Server must still serve new connections afterward.
	c2 := dial(t)
	sendRecvExact(t, c2, cmd("PING"), "+PONG\r\n")
}

func TestNegativeBulkLengthRejected(t *testing.T) {
	// A client sending $-5 inside a command array is a protocol error.
	c := dial(t)
	c.Write([]byte("*1\r\n$-5\r\n"))
	line := readLine(t, c)
	if !strings.HasPrefix(line, "-ERR") {
		t.Fatalf("expected -ERR protocol error, got %q", line)
	}
}
