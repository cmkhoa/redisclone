// Package aof implements the append-only file: a log of every mutation the
// keyspace has accepted, replayed at startup to rebuild it.
//
// The log is a stream of RESP arrays of bulk strings — the same encoding
// clients speak, and the same one real Redis uses for its AOF. That is not
// nostalgia: it means the log is written by the encoder that M0 already tested,
// read by the parser that M0 already tested, and readable by redis-cli's
// --pipe. A bespoke binary format would have been a third serialisation to get
// right and to keep in sync.
//
// What is logged is *effects*, not the commands clients sent. See DECISIONS.md;
// the short version is that "SET k v EX 60" means something different an hour
// later, so the store logs the absolute deadline it actually applied.
package aof

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"redisclone/internal/resp"
)

// Policy says when the log is forced to stable storage.
type Policy string

const (
	// PolicyAlways fsyncs before the client is told the write succeeded. No
	// acknowledged write is ever lost; costs an fsync per write command.
	PolicyAlways Policy = "always"

	// PolicyEverysec fsyncs on a one-second timer. A crash loses at most the
	// last second of writes. This is the default, and the trade real
	// deployments almost always take.
	PolicyEverysec Policy = "everysec"

	// PolicyNo never fsyncs: the data reaches the kernel and is written out
	// whenever the OS feels like it (typically ~30s on Linux). Survives a
	// process crash, not a machine crash.
	PolicyNo Policy = "no"
)

func ParsePolicy(s string) (Policy, error) {
	switch p := Policy(s); p {
	case PolicyAlways, PolicyEverysec, PolicyNo:
		return p, nil
	default:
		return "", fmt.Errorf("unknown fsync policy %q (want always, everysec or no)", s)
	}
}

// SyncInterval is how often the background loop flushes and, for everysec,
// fsyncs.
const SyncInterval = time.Second

// Log is an open append-only file.
//
// Safe for concurrent use. Append is called with the keyspace lock held (that
// is what keeps the log's order identical to the order mutations actually
// happened in), so it does the cheapest possible thing: encode into a buffer.
// Flushing and fsyncing happen in Sync, which is deliberately never called with
// the keyspace lock held — an fsync can take milliseconds, and holding the
// keyspace for that long would stall every client on the server.
type Log struct {
	mu     sync.Mutex
	f      *os.File
	bw     *bufio.Writer
	w      *resp.Writer
	policy Policy
	path   string

	// rewriting mirrors every append into rewriteTail while a background
	// rewrite builds its snapshot. FinishRewrite appends this tail before the
	// atomic rename, so no mutation that raced with the snapshot is lost.
	rewriting   bool
	rewriteTail bytes.Buffer

	// err latches the first write or fsync failure. Once the log is broken,
	// every subsequent write command is refused rather than being accepted
	// into memory and silently lost — see the MISCONF check in the server.
	err error
}

// Open opens (creating if needed) the append-only file at path for appending.
//
// It does not read the file: replaying happens first, through Replay, before
// anything is appended.
func Open(path string, policy Policy) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open aof: %w", err)
	}
	bw := bufio.NewWriterSize(f, 64*1024)
	return &Log{f: f, bw: bw, w: resp.NewWriter(bw), policy: policy, path: path}, nil
}

// Policy returns the log's fsync policy.
func (l *Log) Policy() Policy { return l.policy }

// Append encodes one command into the log's buffer.
//
// Buffer only: no write(2), no fsync. The caller holds the keyspace lock, and
// the whole point of this being cheap is that the lock is released again
// immediately.
func (l *Log) Append(args ...[]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.err != nil {
		return l.err
	}
	if err := writeCommand(l.w, args); err != nil {
		return l.fail(err)
	}
	if l.rewriting {
		if err := writeCommand(resp.NewWriter(&l.rewriteTail), args); err != nil {
			return l.fail(err)
		}
	}
	return nil
}

func writeCommand(w *resp.Writer, args [][]byte) error {
	if err := w.WriteArrayHeader(len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if err := w.WriteBulkString(a); err != nil {
			return err
		}
	}
	return nil
}

// BeginRewrite starts retaining every new mutation for a replacement AOF.
// Call Snapshot only after this succeeds; that ordering is what makes writes
// concurrent with the snapshot appear in the final file.
func (l *Log) BeginRewrite() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	if l.rewriting {
		return fmt.Errorf("AOF rewrite already in progress")
	}
	l.rewriting = true
	l.rewriteTail.Reset()
	return nil
}

// FinishRewrite atomically replaces the AOF with snapshot followed by all
// mutations appended since BeginRewrite. snapshot is a stream of command args.
func (l *Log) FinishRewrite(snapshot [][][]byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(l.path), ".redisclone-rewrite-*")
	if err != nil {
		l.abortRewrite()
		return fmt.Errorf("create rewrite AOF: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	bw := bufio.NewWriterSize(tmp, 64*1024)
	w := resp.NewWriter(bw)
	for _, args := range snapshot {
		if err := writeCommand(w, args); err != nil {
			tmp.Close()
			l.abortRewrite()
			return fmt.Errorf("write rewrite snapshot: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		tmp.Close()
		l.abortRewrite()
		return fmt.Errorf("flush rewrite snapshot: %w", err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.rewriting {
		tmp.Close()
		return fmt.Errorf("AOF rewrite is not in progress")
	}
	if l.err != nil {
		tmp.Close()
		l.rewriting = false
		l.rewriteTail.Reset()
		return l.err
	}
	if _, err := tmp.Write(l.rewriteTail.Bytes()); err != nil {
		tmp.Close()
		l.rewriting = false
		l.rewriteTail.Reset()
		return fmt.Errorf("write rewrite tail: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		l.rewriting = false
		l.rewriteTail.Reset()
		return fmt.Errorf("sync rewrite AOF: %w", err)
	}
	if err := tmp.Close(); err != nil {
		l.rewriting = false
		l.rewriteTail.Reset()
		return fmt.Errorf("close rewrite AOF: %w", err)
	}
	if err := os.Rename(tmpName, l.path); err != nil {
		l.rewriting = false
		l.rewriteTail.Reset()
		return fmt.Errorf("install rewrite AOF: %w", err)
	}

	newFile, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		l.rewriting = false
		l.rewriteTail.Reset()
		return l.fail(fmt.Errorf("reopen rewritten AOF: %w", err))
	}
	oldFile := l.f
	l.f = newFile
	l.bw = bufio.NewWriterSize(newFile, 64*1024)
	l.w = resp.NewWriter(l.bw)
	l.rewriting = false
	l.rewriteTail.Reset()
	_ = oldFile.Close() // the replacement is synced and installed already.
	return nil
}

func (l *Log) abortRewrite() {
	l.mu.Lock()
	l.rewriting = false
	l.rewriteTail.Reset()
	l.mu.Unlock()
}

// Sync flushes the buffer to the kernel and, unless the policy says otherwise,
// asks the kernel to put it on the disk.
//
// Must not be called with the keyspace lock held.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.syncLocked(l.policy != PolicyNo)
}

// Flush pushes the buffer to the kernel without an fsync. Enough to survive
// this process dying; not enough to survive the machine dying.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.syncLocked(false)
}

func (l *Log) syncLocked(fsync bool) error {
	if l.err != nil {
		return l.err
	}
	if err := l.bw.Flush(); err != nil {
		return l.fail(err)
	}
	if fsync {
		if err := l.f.Sync(); err != nil {
			return l.fail(err)
		}
	}
	return nil
}

// SyncIfAlways is the per-command durability hook: under the always policy it
// fsyncs, under the others it does nothing (their fsyncs are on the timer).
//
// The server calls it after a write command has been applied and before the
// reply reaches the client, which is what "no acknowledged write is lost"
// actually requires. Acknowledging first and syncing later would be a lie the
// client cannot detect.
func (l *Log) SyncIfAlways() error {
	if l.policy != PolicyAlways {
		return nil
	}
	return l.Sync()
}

// RunSyncLoop flushes (and, for everysec, fsyncs) on a timer until ctx is done.
//
// Under PolicyNo this still runs: the buffer belongs to this process, so
// leaving data in it would make a *process* crash lose writes that the policy
// promises to keep. Flushing to the kernel and never fsyncing is exactly what
// "no" is supposed to mean.
func (l *Log) RunSyncLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = SyncInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Last flush on the way out: a clean shutdown should not lose
			// writes that a crash would not have lost either.
			_ = l.Sync()
			return
		case <-ticker.C:
			l.mu.Lock()
			_ = l.syncLocked(l.policy == PolicyEverysec)
			l.mu.Unlock()
		}
	}
}

// Err reports the latched write error, if the log has failed.
func (l *Log) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Close flushes, fsyncs and closes. Always fsyncs regardless of policy: this is
// an orderly shutdown, and there is no reason to discard what we have.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	syncErr := l.syncLocked(true)
	closeErr := l.f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// fail latches err and returns it.
func (l *Log) fail(err error) error {
	if l.err == nil {
		l.err = err
	}
	return l.err
}
