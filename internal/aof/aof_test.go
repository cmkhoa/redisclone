package aof

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func tempLog(t *testing.T, policy Policy) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	l, err := Open(path, policy)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l, path
}

func bulks(args ...string) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = []byte(a)
	}
	return out
}

// The log is RESP, so the bytes on disk are checkable against the spec by hand
// — and are exactly what a client would have sent.
func TestAppendWritesRESP(t *testing.T) {
	l, path := tempLog(t, PolicyEverysec)

	if err := l.Append(bulks("SET", "k", "v")...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Append(bulks("DEL", "k")...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Nothing is required to be on disk before a sync...
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n" + "*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"
	if string(got) != want {
		t.Errorf("log contents:\n got:  %q\n want: %q", got, want)
	}
}

// Values are arbitrary bytes, and the log has to survive them: a value
// containing CRLF must not be able to forge a record boundary.
func TestAppendIsBinarySafe(t *testing.T) {
	l, path := tempLog(t, PolicyEverysec)
	value := "a\r\n*1\r\n$4\r\nPING\r\n\x00b"

	if err := l.Append(bulks("SET", "k", value)...); err != nil {
		t.Fatal(err)
	}
	l.Sync()

	var got [][]byte
	res, err := Replay(path, func(args [][]byte) error { got = args; return nil })
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Commands != 1 {
		t.Fatalf("replayed %d commands, want 1 — a value forged a record boundary", res.Commands)
	}
	if len(got) != 3 || string(got[2]) != value {
		t.Errorf("value did not round-trip: %q", got)
	}
}

// The offset arithmetic Replay uses to find a torn tail assumes EncodedLen
// predicts exactly what Append writes. If that ever drifts, truncation would
// cut the file in the wrong place.
func TestEncodedLenMatchesWhatIsWritten(t *testing.T) {
	l, path := tempLog(t, PolicyEverysec)

	var want int64
	for i := 0; i < 50; i++ {
		cmd := bulks("SET", fmt.Sprintf("key:%d", i), strings.Repeat("v", i))
		if err := l.Append(cmd...); err != nil {
			t.Fatal(err)
		}
		want += EncodedLen(cmd)
	}
	l.Sync()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != want {
		t.Errorf("file is %d bytes, EncodedLen predicted %d", fi.Size(), want)
	}
}

func TestReplayMissingFileIsNotAnError(t *testing.T) {
	res, err := Replay(filepath.Join(t.TempDir(), "nope.aof"), func([][]byte) error {
		t.Fatal("apply called for a file that does not exist")
		return nil
	})
	if err != nil {
		t.Errorf("Replay on a missing file: %v", err)
	}
	if res.Commands != 0 {
		t.Errorf("Commands = %d, want 0", res.Commands)
	}
}

func TestReplayAppliesInOrder(t *testing.T) {
	l, path := tempLog(t, PolicyEverysec)
	for _, v := range []string{"first", "second", "third"} {
		if err := l.Append(bulks("SET", "k", v)...); err != nil {
			t.Fatal(err)
		}
	}
	l.Sync()

	var seen []string
	res, err := Replay(path, func(args [][]byte) error {
		seen = append(seen, string(args[2]))
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Commands != 3 || strings.Join(seen, ",") != "first,second,third" {
		t.Errorf("replayed %v (%d commands), want first,second,third in order", seen, res.Commands)
	}
	if res.Truncated != 0 {
		t.Errorf("Truncated = %d on an intact log, want 0", res.Truncated)
	}
}

// A crash part-way through a write leaves a half-record at the end. That must
// cost the torn command and nothing else: refusing to start would turn an
// ordinary power cut into an unbootable server.
func TestReplayTruncatesATornTail(t *testing.T) {
	for _, tail := range []string{
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhal", // died mid-payload
		"*3\r\n$3\r\nSET\r\n",                     // died after the header
		"*",                                       // died on the first byte
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r", // died one byte from done
	} {
		t.Run(fmt.Sprintf("%q", tail), func(t *testing.T) {
			l, path := tempLog(t, PolicyEverysec)
			good := bulks("SET", "survivor", "v")
			if err := l.Append(good...); err != nil {
				t.Fatal(err)
			}
			l.Sync()
			l.Close()

			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			f.WriteString(tail)
			f.Close()

			var applied int
			res, err := Replay(path, func([][]byte) error { applied++; return nil })
			if err != nil {
				t.Fatalf("Replay: %v", err)
			}
			if applied != 1 {
				t.Errorf("applied %d commands, want the 1 complete one", applied)
			}
			if res.Truncated != int64(len(tail)) {
				t.Errorf("Truncated = %d, want %d", res.Truncated, len(tail))
			}
			// The file must actually be repaired, not just read past: the next
			// append has to land on a record boundary.
			fi, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if fi.Size() != EncodedLen(good) {
				t.Errorf("file is %d bytes after repair, want %d", fi.Size(), EncodedLen(good))
			}
		})
	}
}

// Damage anywhere but the tail cannot be explained by a crash, so replay
// refuses instead of guessing which half of the file to believe.
func TestReplayRefusesCorruption(t *testing.T) {
	l, path := tempLog(t, PolicyEverysec)
	l.Append(bulks("SET", "a", "1")...)
	l.Sync()
	l.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.WriteString("this is not resp\r\n*2\r\n$3\r\nDEL\r\n$1\r\na\r\n")
	f.Close()

	before, _ := os.Stat(path)
	_, err := Replay(path, func([][]byte) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Replay = %v, want ErrCorrupt", err)
	}
	// And it must not have "repaired" anything: a human needs to look at this.
	after, _ := os.Stat(path)
	if before.Size() != after.Size() {
		t.Errorf("corrupt log was modified: %d -> %d bytes", before.Size(), after.Size())
	}
}

// A command that the caller rejects (replay's own validation) is corruption
// too, not something to skip past.
func TestReplayPropagatesApplyErrors(t *testing.T) {
	l, path := tempLog(t, PolicyEverysec)
	l.Append(bulks("SET", "a", "1")...)
	l.Append(bulks("GET", "a")...) // not a write command
	l.Sync()

	_, err := Replay(path, func(args [][]byte) error {
		if string(args[0]) == "GET" {
			return errors.New("not a write command")
		}
		return nil
	})
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("Replay = %v, want ErrCorrupt", err)
	}
}

func TestParsePolicy(t *testing.T) {
	for _, s := range []string{"always", "everysec", "no"} {
		if _, err := ParsePolicy(s); err != nil {
			t.Errorf("ParsePolicy(%q): %v", s, err)
		}
	}
	if _, err := ParsePolicy("sometimes"); err == nil {
		t.Error("ParsePolicy accepted a policy that does not exist")
	}
}

// Under "always" the data is on disk before the call returns — that is the
// whole promise, and it is what the server relies on when it fsyncs before
// replying.
func TestSyncIfAlwaysHonoursThePolicy(t *testing.T) {
	t.Run("always syncs", func(t *testing.T) {
		l, path := tempLog(t, PolicyAlways)
		l.Append(bulks("SET", "k", "v")...)
		if err := l.SyncIfAlways(); err != nil {
			t.Fatal(err)
		}
		if fi, _ := os.Stat(path); fi.Size() == 0 {
			t.Error("nothing reached the file under the always policy")
		}
	})

	t.Run("everysec does not sync per command", func(t *testing.T) {
		l, path := tempLog(t, PolicyEverysec)
		l.Append(bulks("SET", "k", "v")...)
		if err := l.SyncIfAlways(); err != nil {
			t.Fatal(err)
		}
		// Still buffered: everysec's whole point is that the write path does
		// not touch the disk.
		if fi, _ := os.Stat(path); fi.Size() != 0 {
			t.Error("everysec wrote through on the command path")
		}
	})
}

func TestRunSyncLoopFlushesAndStops(t *testing.T) {
	l, path := tempLog(t, PolicyEverysec)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { defer close(done); l.RunSyncLoop(ctx, 5*time.Millisecond) }()

	l.Append(bulks("SET", "k", "v")...)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fi, _ := os.Stat(path); fi.Size() > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if fi, _ := os.Stat(path); fi.Size() == 0 {
		t.Error("the sync loop never flushed the buffer")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("RunSyncLoop did not return when its context was cancelled")
	}
}

// A broken log must stay broken and must say so: the server turns this into a
// -MISCONF refusal rather than accepting writes it cannot persist.
func TestWriteErrorsAreLatched(t *testing.T) {
	l, _ := tempLog(t, PolicyEverysec)

	// Closing the file underneath the log is the cheapest way to make every
	// subsequent write fail the way a full disk would.
	l.f.Close()

	// Enough data to force the buffer through to the (closed) file.
	big := strings.Repeat("x", 128*1024)
	err := l.Append(bulks("SET", "k", big)...)
	if err == nil {
		if err = l.Sync(); err == nil {
			t.Fatal("writing to a closed file succeeded")
		}
	}
	if l.Err() == nil {
		t.Error("Err() is nil after a failed write; the failure was not latched")
	}
	if err := l.Append(bulks("SET", "k", "v")...); err == nil {
		t.Error("Append succeeded after the log had already failed")
	}
}

func TestRewriteAppendsConcurrentTail(t *testing.T) {
	l, path := tempLog(t, PolicyEverysec)
	if err := l.BeginRewrite(); err != nil {
		t.Fatal(err)
	}
	// This write races with snapshot construction in the real server. It must
	// appear after the snapshot record in the replacement log.
	if err := l.Append(bulks("SET", "k", "new")...); err != nil {
		t.Fatal(err)
	}
	if err := l.FinishRewrite([][][]byte{bulks("SET", "k", "old")}); err != nil {
		t.Fatal(err)
	}

	var records []string
	if _, err := Replay(path, func(args [][]byte) error {
		records = append(records, strings.Join(func() []string {
			out := make([]string, len(args))
			for i, a := range args {
				out[i] = string(a)
			}
			return out
		}(), " "))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(records, ","), "SET k old,SET k new"; got != want {
		t.Errorf("rewritten records = %q, want %q", got, want)
	}
}
