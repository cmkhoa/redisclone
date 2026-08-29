// M3 black-box tests: durability. Unlike the other suites, these start their
// own server processes — the whole point is what survives one dying.
//
// Same rule as ever: no RESP parser in here.
package e2e

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// durableServer starts a server with the append-only file switched on, in a
// directory of the test's own, and returns a handle that can restart it.
type durableServer struct {
	t      *testing.T
	dir    string
	addr   string
	fsync  string
	cmd    *exec.Cmd
	stderr *os.File
}

func startDurable(t *testing.T, fsync string) *durableServer {
	t.Helper()
	d := &durableServer{
		t:     t,
		dir:   t.TempDir(),
		addr:  fmt.Sprintf("127.0.0.1:%d", freePort()),
		fsync: fsync,
	}
	d.start()
	t.Cleanup(d.stop)
	return d
}

func (d *durableServer) start() {
	d.t.Helper()
	cmd := exec.Command(serverBin, "-addr", d.addr, "-appendonly", "-dir", d.dir, "-appendfsync", d.fsync)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		d.t.Fatalf("start server: %v", err)
	}
	d.cmd = cmd
	if !waitForPort(d.addr, 5*time.Second) {
		cmd.Process.Kill()
		d.t.Fatalf("server never opened %s", d.addr)
	}
}

// stop shuts the server down politely, so buffered writes are flushed — the
// everysec policy's promise is about crashes, and a clean shutdown is not one.
func (d *durableServer) stop() {
	if d.cmd == nil || d.cmd.Process == nil {
		return
	}
	d.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { d.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		d.cmd.Process.Kill()
		<-done
	}
	d.cmd = nil
}

// kill terminates the server without warning: no flush, no fsync, nothing. What
// survives this is what the fsync policy actually guarantees.
func (d *durableServer) kill() {
	d.t.Helper()
	if d.cmd == nil || d.cmd.Process == nil {
		return
	}
	d.cmd.Process.Kill()
	d.cmd.Wait()
	d.cmd = nil
}

func (d *durableServer) restart() {
	d.t.Helper()
	d.stop()
	d.start()
}

// conn dials this server, with the usual deadline.
func (d *durableServer) conn() net.Conn {
	d.t.Helper()
	c, err := net.Dial("tcp", d.addr)
	if err != nil {
		d.t.Fatalf("dial %s: %v", d.addr, err)
	}
	d.t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(10 * time.Second))
	return c
}

func (d *durableServer) aofPath() string { return filepath.Join(d.dir, "appendonly.aof") }

// The headline: the keyspace survives a restart.
func TestDataSurvivesARestart(t *testing.T) {
	d := startDurable(t, "everysec")
	c := d.conn()

	sendRecvExact(t, c, cmd("SET", "plain", "value"), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", "binary", "a\r\nb\x00c"), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", "empty", ""), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", "overwritten", "first"), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", "overwritten", "second"), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", "deleted", "x"), "+OK\r\n")
	sendRecvExact(t, c, cmd("DEL", "deleted"), ":1\r\n")

	d.restart()
	c = d.conn()

	sendRecvExact(t, c, cmd("GET", "plain"), bulk("value"))
	sendRecvExact(t, c, cmd("GET", "binary"), bulk("a\r\nb\x00c"))
	sendRecvExact(t, c, cmd("GET", "empty"), "$0\r\n\r\n")
	sendRecvExact(t, c, cmd("EXISTS", "empty"), ":1\r\n")
	sendRecvExact(t, c, cmd("GET", "overwritten"), bulk("second"))
	sendRecvExact(t, c, cmd("GET", "deleted"), "$-1\r\n")
}

// TTLs survive as deadlines, not as durations: a key restarted after 2 seconds
// has 2 seconds less to live, and one whose deadline passed while the server
// was down does not come back.
func TestExpirySurvivesARestart(t *testing.T) {
	d := startDurable(t, "everysec")
	c := d.conn()

	sendRecvExact(t, c, cmd("SET", "long", "v", "EX", "1000"), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", "short", "v", "PX", "300"), "+OK\r\n")
	sendRecvExact(t, c, cmd("SET", "persistent", "v"), "+OK\r\n")

	before := sendRecvInt(t, c, cmd("PTTL", "long"))

	time.Sleep(600 * time.Millisecond) // "short" dies during this
	d.restart()
	c = d.conn()

	// The deadline kept ticking across the restart rather than resetting.
	after := sendRecvInt(t, c, cmd("PTTL", "long"))
	if after >= before {
		t.Errorf("PTTL was %d before the restart and %d after; the deadline was reset", before, after)
	}
	if after < 900_000 {
		t.Errorf("PTTL = %d after the restart, want a little under 1000000", after)
	}

	sendRecvExact(t, c, cmd("GET", "short"), "$-1\r\n")
	sendRecvExact(t, c, cmd("TTL", "short"), ":-2\r\n")
	sendRecvExact(t, c, cmd("GET", "persistent"), bulk("v"))
	sendRecvExact(t, c, cmd("TTL", "persistent"), ":-1\r\n")
}

// Restarting repeatedly must not drift: a key with a TTL that is replayed,
// re-logged, replayed again... must still expire at the original time.
func TestRepeatedRestartsDoNotResetDeadlines(t *testing.T) {
	d := startDurable(t, "everysec")
	c := d.conn()
	sendRecvExact(t, c, cmd("SET", "k", "v", "EX", "600"), "+OK\r\n")

	last := sendRecvInt(t, c, cmd("PTTL", "k"))
	for i := 0; i < 3; i++ {
		time.Sleep(150 * time.Millisecond)
		d.restart()
		c = d.conn()

		got := sendRecvInt(t, c, cmd("PTTL", "k"))
		if got >= last {
			t.Fatalf("restart %d: PTTL went from %d to %d — the deadline is being renewed", i, last, got)
		}
		last = got
	}
}

// Under "always", an acknowledged write is on the disk even if the process is
// killed the instant afterwards. This is the policy's entire reason to exist.
func TestAlwaysPolicySurvivesAKill(t *testing.T) {
	d := startDurable(t, "always")
	c := d.conn()

	for i := 0; i < 20; i++ {
		sendRecvExact(t, c, cmd("SET", fmt.Sprintf("k:%d", i), fmt.Sprintf("v%d", i)), "+OK\r\n")
	}

	d.kill() // no signal handler, no flush, no fsync
	d.start()
	c = d.conn()

	for i := 0; i < 20; i++ {
		sendRecvExact(t, c, cmd("GET", fmt.Sprintf("k:%d", i)), bulk(fmt.Sprintf("v%d", i)))
	}
}

// A crash part-way through a write leaves a half-written record. The server
// must start, discard exactly that record, and keep everything before it.
func TestTornTailIsRepairedOnStartup(t *testing.T) {
	d := startDurable(t, "always")
	c := d.conn()
	sendRecvExact(t, c, cmd("SET", "survivor", "v"), "+OK\r\n")
	d.kill()

	// Simulate the crash: append a record that stops half way through.
	f, err := os.OpenFile(d.aofPath(), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("*3\r\n$3\r\nSET\r\n$4\r\nhalf\r\n$5\r\nwri"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	d.start()
	c = d.conn()
	sendRecvExact(t, c, cmd("GET", "survivor"), bulk("v"))
	sendRecvExact(t, c, cmd("GET", "half"), "$-1\r\n")

	// And the repaired log must still be appendable: a new write, another
	// restart, and both keys are there.
	sendRecvExact(t, c, cmd("SET", "after", "repair"), "+OK\r\n")
	d.restart()
	c = d.conn()
	sendRecvExact(t, c, cmd("GET", "survivor"), bulk("v"))
	sendRecvExact(t, c, cmd("GET", "after"), bulk("repair"))
}

// Corruption that a crash cannot explain is refused: the server exits rather
// than serving a keyspace that disagrees with its own log.
func TestCorruptLogRefusesToStart(t *testing.T) {
	d := startDurable(t, "always")
	c := d.conn()
	sendRecvExact(t, c, cmd("SET", "k", "v"), "+OK\r\n")
	d.kill()

	f, err := os.OpenFile(d.aofPath(), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("this is not resp\r\n" + cmd("SET", "later", "v"))
	f.Close()

	out, err := exec.Command(serverBin, "-addr", d.addr, "-appendonly", "-dir", d.dir).CombinedOutput()
	if err == nil {
		t.Fatal("the server started on a corrupt log")
	}
	if !strings.Contains(string(out), "corrupt") {
		t.Errorf("startup output does not mention corruption: %s", out)
	}
}

// Durability is off by default: no file appears unless it is asked for.
func TestNoLogWithoutAppendonly(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort())
	cmdProc := exec.Command(serverBin, "-addr", addr, "-dir", dir)
	cmdProc.Stderr = os.Stderr
	if err := cmdProc.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmdProc.Process.Kill(); cmdProc.Wait() }()

	if !waitForPort(addr, 5*time.Second) {
		t.Fatal("server never started")
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	sendRecvExact(t, c, cmd("SET", "k", "v"), "+OK\r\n")

	if entries, err := os.ReadDir(dir); err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Errorf("directory is not empty: %v", entries)
	}
}
