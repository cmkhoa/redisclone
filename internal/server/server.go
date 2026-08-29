// Package server accepts connections and runs the command loop for each one.
package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
	"strings"

	"redisclone/internal/aof"
	"redisclone/internal/resp"
	"redisclone/internal/store"
)

// writeBufSize is the reply buffer per connection. Small enough that ten
// thousand idle connections is a few tens of MiB, big enough that a typical
// reply is one syscall.
const writeBufSize = 16 * 1024

// Server holds what is shared across connections: the keyspace, and the
// logger.
//
// Everything in here is touched by every connection goroutine at once, so
// everything in here has to be safe for concurrent use. Today that is one
// field with its own lock inside it (*store.Store); the moment a second piece
// of shared mutable state shows up, the question of how the two stay
// consistent with each other becomes a real one.
type Server struct {
	store *store.Store
	// aof is nil when durability is switched off, which is the only reason
	// every use of it is guarded.
	aof    *aof.Log
	logger *log.Logger
}

func New(logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	return &Server{store: store.New(), logger: logger}
}

// Store exposes the keyspace for tests.
func (s *Server) Store() *store.Store { return s.store }

// AttachAOF makes the server durable: every mutation from now on is journalled,
// and write commands are refused if the journal fails.
//
// Call it after ReplayAOF and before serving clients. Attaching while clients
// are connected would leave the log missing everything written before it, which
// is worse than no log at all — it would replay into a keyspace that silently
// lacks its own history.
func (s *Server) AttachAOF(l *aof.Log) {
	s.aof = l
	s.store.SetJournal(l)
}

// ReplayAOF rebuilds the keyspace from the log at path, if it exists.
//
// The journal is not attached yet, so nothing read here is written back out.
func (s *Server) ReplayAOF(path string) (aof.Result, error) {
	return aof.Replay(path, s.applyLogged)
}

// applyLogged executes one command read back from the log.
//
// It runs the real handlers rather than a separate interpreter for logged
// commands. A second interpreter would be a second implementation of SET's
// semantics, free to drift from the first — and the failure mode of that drift
// is a keyspace that is subtly wrong after every restart.
//
// What it does add is validation the wire protocol does not need: the log is
// supposed to contain write commands this server wrote, so anything else in
// there means the file is not what we think it is, and replaying it would
// rebuild a keyspace that never existed.
func (s *Server) applyLogged(args [][]byte) error {
	name := strings.ToUpper(string(args[0]))
	cmd, ok := commands[name]
	switch {
	case !ok:
		return fmt.Errorf("unknown command %q", safeName(name))
	case !cmd.writes:
		return fmt.Errorf("%s is not a write command", safeName(name))
	}

	rest := args[1:]
	if len(rest) < cmd.minArgs || (cmd.maxArgs >= 0 && len(rest) > cmd.maxArgs) {
		return fmt.Errorf("%s: wrong number of arguments (%d)", safeName(name), len(rest))
	}

	// Handlers report command-level failures by writing an error *reply*, not
	// by returning one, so the reply is captured and inspected. Without this a
	// log record the server would reject from a client would be applied in
	// silence at startup.
	var reply bytes.Buffer
	if err := cmd.fn(s, resp.NewWriter(&reply), rest); err != nil {
		return err
	}
	if b := reply.Bytes(); len(b) > 0 && b[0] == '-' {
		return fmt.Errorf("%s: %s", safeName(name), strings.TrimSpace(string(b[1:])))
	}
	return nil
}

// StartBackgroundTasks launches the server's housekeeping goroutines and
// returns once they are running. They stop when ctx is cancelled.
//
// Separate from New so that tests get a server with no timers attached unless
// they ask for them, and separate from Serve so the caller controls the
// lifetime rather than tying it to one listener.
func (s *Server) StartBackgroundTasks(ctx context.Context) {
	// Active expiration: without it, a key written with a TTL and never read
	// again would hold its memory forever. See package store.
	go s.store.RunActiveExpiration(ctx, store.DefaultActiveExpireInterval)

	// The AOF's timed flush/fsync. Under the "always" policy this loop still
	// runs and costs nothing: the buffer is already empty by the time it ticks.
	if s.aof != nil {
		go s.aof.RunSyncLoop(ctx, aof.SyncInterval)
	}
}

// Serve accepts connections until l is closed, handling each in its own
// goroutine.
//
// Goroutine-per-connection, rather than the single-threaded event loop real
// Redis uses: a goroutine blocked on a socket read costs a few KiB of stack,
// not an OS thread, and the Go runtime multiplexes them onto epoll/kqueue
// underneath — so straight-line handler code scales to tens of thousands of
// connections. What it costs us is the guarantee Redis gets for free: commands
// from different connections really do run concurrently, so the store M1 adds
// has to be made safe explicitly. See DECISIONS.md.
func (s *Server) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			// A closed listener ends Serve; per-connection accept failures
			// (an fd limit, a client that vanished during the handshake) must
			// not.
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		go s.HandleConn(conn)
	}
}

// HandleConn runs one connection's read/dispatch/reply loop until the client
// disconnects or sends unparseable bytes.
func (s *Server) HandleConn(conn net.Conn) {
	defer conn.Close()
	defer func() {
		// One client's bug must not take down every other client's server.
		if v := recover(); v != nil {
			s.logger.Printf("panic serving %v: %v\n%s", conn.RemoteAddr(), v, debug.Stack())
		}
	}()

	bw := bufio.NewWriterSize(conn, writeBufSize)
	defer bw.Flush() // the goodbye -ERR of a protocol error has to get out

	// The flush hook: bufio.Reader only touches the socket when its buffer runs
	// dry, which is exactly the moment we are about to block waiting for the
	// client — and therefore the last moment at which pending replies must be
	// on the wire. Hooking the flush here, instead of flushing after every
	// command, means a pipelined batch of commands coalesces into a single
	// write automatically, while a client that sends one command and waits can
	// never deadlock against a reply still sitting in the buffer.
	r := resp.NewReader(flushBeforeRead{src: conn, flush: bw.Flush})
	w := resp.NewWriter(bw)

	for {
		args, err := r.ReadCommand()
		if err != nil {
			s.reportReadError(conn, w, err)
			return
		}
		if len(args) == 0 {
			continue // "*0\r\n": well-formed, nothing to do, no reply
		}
		if err := s.dispatch(w, args); err != nil {
			// Only I/O errors reach here — a broken pipe or a dead client.
			// Command-level errors are replies, not errors.
			return
		}
	}
}

// reportReadError decides what a failed read means for the connection.
//
// Error policy: a command error (bad arity, unknown command) is a normal reply
// and the connection continues. A protocol error is different — the stream
// position is no longer trustworthy, since we no longer know where the next
// command begins — so we say why and hang up. Resynchronising is not possible
// in general: "$100\r\n" with a wrong length makes every following byte
// ambiguous.
func (s *Server) reportReadError(conn net.Conn, w *resp.Writer, err error) {
	var pe *resp.ProtocolError
	switch {
	case errors.Is(err, io.EOF):
		// Clean disconnect at a command boundary. Not an error.
	case errors.As(err, &pe):
		_ = w.WriteError("ERR " + pe.Error())
	case errors.Is(err, io.ErrUnexpectedEOF):
		// Truncated command; nobody left to tell.
	default:
		s.logger.Printf("read from %v: %v", conn.RemoteAddr(), err)
	}
}

// safeName makes a client-supplied command name safe to put in an error reply.
//
// Errors are CRLF-terminated with no length prefix, so a CR or LF echoed back
// from the wire would let a client inject a fake reply into the stream — and
// a 512 MiB "command name" would be reflected back verbatim. Both are handled
// by replacing anything non-printable and truncating.
func safeName(name string) string {
	const max = 128
	truncated := false
	if len(name) > max {
		name, truncated = name[:max], true
	}
	var b strings.Builder
	b.Grow(len(name))
	for i := 0; i < len(name); i++ {
		if c := name[i]; c >= 0x20 && c < 0x7f && c != '\'' {
			b.WriteByte(c)
		} else {
			b.WriteByte('.')
		}
	}
	if truncated {
		b.WriteString("...")
	}
	return b.String()
}

// flushBeforeRead flushes pending replies immediately before a read that could
// block. See HandleConn.
type flushBeforeRead struct {
	src   io.Reader
	flush func() error
}

func (f flushBeforeRead) Read(p []byte) (int, error) {
	if err := f.flush(); err != nil {
		return 0, err
	}
	return f.src.Read(p)
}
