package server

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"redisclone/internal/resp"
	"redisclone/internal/store"
)

// command is one entry in the dispatch table.
//
// minArgs/maxArgs count the arguments *after* the command name; maxArgs of -1
// means variadic. Keeping arity in the table rather than inside each handler
// means the check happens in exactly one place, every handler can index the
// arguments it was promised without re-checking, and a new command cannot
// forget to validate.
//
// Redis encodes the same thing as a single `arity` int (negative meaning "at
// least"), which cannot express "one or two" — PING's actual arity. Two fields
// cost nothing and say what they mean.
type command struct {
	minArgs int
	maxArgs int
	// writes is true for commands that can modify the keyspace. It gates two
	// things durability needs: refusing writes when the append-only file has
	// failed, and fsyncing before the reply under the "always" policy. It is
	// also what lets replay reject a log containing anything that is not a
	// write.
	writes bool
	fn     func(*Server, *resp.Writer, [][]byte) error
}

// commands is the dispatch table, keyed by upper-cased command name.
//
// A map lookup rather than a switch: it is what makes "does this command
// exist" and "what is its arity" answerable without running the handler, which
// COMMAND DOCS and, later, MULTI's queue-time validation both need.
var commands = map[string]command{
	"PING":      {0, 1, false, (*Server).ping},
	"ECHO":      {1, 1, false, (*Server).echo},
	"SET":       {2, 4, true, (*Server).set},
	"GET":       {1, 1, false, (*Server).get},
	"DEL":       {1, -1, true, (*Server).del},
	"EXISTS":    {1, -1, false, (*Server).exists},
	"EXPIRE":    {2, 2, true, (*Server).expire},
	"PEXPIRE":   {2, 2, true, (*Server).pexpire},
	"EXPIREAT":  {2, 2, true, (*Server).expireat},
	"PEXPIREAT": {2, 2, true, (*Server).pexpireat},
	"TTL":       {1, 1, false, (*Server).ttl},
	"PTTL":      {1, 1, false, (*Server).pttl},
	"DBSIZE":    {0, 0, false, (*Server).dbsize},
	"INFO":      {0, 1, false, (*Server).info},
}

// dispatch runs one command and writes its reply. The returned error is an I/O
// error only: a command that fails replies with -ERR and returns nil, because
// from the connection's point of view that is a successful exchange.
//
// Command names are case-insensitive on the wire.
func (s *Server) dispatch(w *resp.Writer, args [][]byte) error {
	name := strings.ToUpper(string(args[0]))
	cmd, ok := commands[name]
	if !ok {
		// Unknown commands must not be fatal: a modern redis-cli opens with
		// HELLO 3 and COMMAND DOCS, sees -ERR, and quietly falls back to RESP2.
		return w.WriteError("ERR unknown command '" + safeName(name) + "'")
	}

	args = args[1:]
	if len(args) < cmd.minArgs || (cmd.maxArgs >= 0 && len(args) > cmd.maxArgs) {
		return wrongArity(w, name)
	}

	if cmd.writes && s.aof != nil {
		// The log has failed (a full disk, most likely). Refuse the write
		// rather than accepting it into memory and reporting success for data
		// that will not survive a restart. Reads keep working: a degraded
		// cache is more useful than a dead one.
		if err := s.aof.Err(); err != nil {
			s.logger.Printf("refusing write: aof is failed: %v", err)
			return w.WriteError("MISCONF Errors writing to the AOF file")
		}
	}
	if cmd.writes {
		// The previous command may have put us one entry over the budget. Do
		// the eviction decision before this mutation, so a noeviction refusal
		// leaves the requested write completely unapplied.
		if _, ok := s.store.EnforceMemoryLimit(); !ok {
			return w.WriteError("OOM command not allowed when used memory > 'maxmemory'.")
		}
	}

	if err := cmd.fn(s, w, args); err != nil {
		return err
	}

	if cmd.writes && s.aof != nil {
		// Under the "always" policy this fsyncs. It happens here — after the
		// mutation, before the reply leaves the buffer for the socket — because
		// that ordering is the entire promise of the policy: nothing is
		// acknowledged that is not already on the disk.
		if err := s.aof.SyncIfAlways(); err != nil {
			s.logger.Printf("aof sync: %v", err)
		}
	}
	return nil
}

func wrongArity(w *resp.Writer, name string) error {
	return w.WriteError("ERR wrong number of arguments for '" + strings.ToLower(safeName(name)) + "' command")
}

// --- connection commands -------------------------------------------------

// ping: bare PING is a status reply; PING with a message echoes it as a bulk
// string. (Real Redis does exactly this, and clients depend on it.)
func (s *Server) ping(w *resp.Writer, args [][]byte) error {
	if len(args) == 1 {
		return w.WriteBulkString(args[0])
	}
	return w.WriteSimpleString("PONG")
}

func (s *Server) echo(w *resp.Writer, args [][]byte) error {
	return w.WriteBulkString(args[0])
}

// --- keyspace commands ---------------------------------------------------

// set: SET key value [EX seconds | PX milliseconds] -> +OK.
//
// The remaining options (NX, XX, KEEPTTL, GET) are still absent, and an
// unrecognised one is an error rather than something silently ignored: a client
// that asks for NX and gets an unconditional overwrite is worse off than one
// that gets -ERR.
func (s *Server) set(w *resp.Writer, args [][]byte) error {
	// string(args[0]) copies the key — unavoidable, since the map owns its
	// keys and args[0] is a buffer the parser handed us. The value is *not*
	// copied: the store takes ownership of the slice, which is safe because
	// the parser allocates a fresh one per argument and nothing mutates a
	// stored value in place. See the invariant at the top of package store.
	key, val := string(args[0]), args[1]

	switch len(args) {
	case 2:
		// A plain SET clears any existing TTL, as in Redis.
		s.store.Set(key, val)
		return w.WriteSimpleString("OK")

	case 4:
		opt, ok := expiryOptions[strings.ToUpper(string(args[2]))]
		if !ok {
			return w.WriteError(errSyntax)
		}
		n, ok := parseTTL(args[3], opt.unit)
		if !ok {
			return w.WriteError("ERR value is not an integer or out of range")
		}
		if n <= 0 {
			// A non-positive expiry is rejected rather than treated as "store
			// then immediately delete": it is almost always a bug in the caller
			// (an already-elapsed relative deadline, or an uninitialised
			// variable), and silently discarding the write hides it. For the
			// absolute forms this rejects timestamps at or before the epoch,
			// which are nonsense rather than merely stale.
			return w.WriteError("ERR invalid expire time in 'set' command")
		}
		if opt.absolute {
			// A deadline in the past is legal here and means the key is
			// already gone — which is exactly what replaying yesterday's log
			// has to do with yesterday's short-lived keys.
			s.store.SetWithDeadline(key, val, epochPlus(n))
		} else {
			s.store.SetWithTTL(key, val, n)
		}
		return w.WriteSimpleString("OK")

	default:
		// Three arguments: an option was given without its value.
		return w.WriteError(errSyntax)
	}
}

// get: GET key -> the value as a bulk string, or the null bulk string.
//
// A missing key and a key holding the empty string are different replies
// ($-1 vs $0\r\n\r\n), and clients distinguish them.
func (s *Server) get(w *resp.Writer, args [][]byte) error {
	// No allocation here: the compiler special-cases map lookups keyed by
	// string([]byte) and skips the copy.
	v, ok := s.store.Get(string(args[0]))
	if !ok {
		return w.WriteNullBulkString()
	}
	return w.WriteBulkString(v)
}

// del: DEL key [key ...] -> number of keys that existed.
func (s *Server) del(w *resp.Writer, args [][]byte) error {
	return w.WriteInteger(int64(s.store.Del(keys(args)...)))
}

// exists: EXISTS key [key ...] -> number of keys that exist, counting
// duplicates (EXISTS k k on an existing k is 2, as in real Redis).
func (s *Server) exists(w *resp.Writer, args [][]byte) error {
	return w.WriteInteger(int64(s.store.Exists(keys(args)...)))
}

func (s *Server) dbsize(w *resp.Writer, _ [][]byte) error {
	return w.WriteInteger(int64(s.store.DBSize()))
}

// info reports the small, operationally useful subset of Redis INFO that this
// server can state honestly. Its intentionally plain text payload is RESP2's
// normal bulk-string response.
func (s *Server) info(w *resp.Writer, args [][]byte) error {
	section := "all"
	if len(args) == 1 {
		section = strings.ToLower(string(args[0]))
	}
	st := s.store.Stats()
	var out strings.Builder
	write := func(name, body string) {
		if section == "all" || section == name {
			out.WriteString(body)
		}
	}
	write("memory", fmt.Sprintf("# Memory\r\nused_memory:%d\r\nmaxmemory:%d\r\nmaxmemory_policy:%s\r\n",
		s.store.Used(), s.store.MaxMemory(), s.store.Policy()))
	write("stats", fmt.Sprintf("# Stats\r\nkeyspace_hits:%d\r\nkeyspace_misses:%d\r\nevicted_keys:%d\r\nexpired_keys:%d\r\n",
		st.KeyspaceHits.Load(), st.KeyspaceMisses.Load(), st.EvictedKeys.Load(), st.ExpiredKeys.Load()))
	aofEnabled := 0
	if s.aof != nil {
		aofEnabled = 1
	}
	write("persistence", fmt.Sprintf("# Persistence\r\naof_enabled:%d\r\n", aofEnabled))
	write("keyspace", fmt.Sprintf("# Keyspace\r\ndb0:keys=%d\r\n", s.store.DBSize()))
	return w.WriteBulkString([]byte(out.String()))
}

// keys converts command arguments to map keys.
//
// The conversion to string is what the map demands, and it copies. Passing the
// whole batch to the store in one call is the point: one lock acquisition for
// a multi-key DEL instead of one per key, and the command is then atomic with
// respect to other commands.
func keys(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}

// --- expiration commands -------------------------------------------------

// errSyntax is Redis's reply to a well-formed command with arguments that do
// not make sense together — as opposed to the wrong *number* of arguments,
// which is the arity error the dispatch table produces.
const errSyntax = "ERR syntax error"

// expiryOption describes one of SET's expiry options: the unit its argument is
// counted in, and whether that argument is a duration from now or an absolute
// point in time.
type expiryOption struct {
	unit     time.Duration
	absolute bool
}

var expiryOptions = map[string]expiryOption{
	"EX":   {time.Second, false},
	"PX":   {time.Millisecond, false},
	"EXAT": {time.Second, true},
	"PXAT": {time.Millisecond, true},
}

// expire: EXPIRE key seconds -> 1 if the key was there to expire, 0 if not.
func (s *Server) expire(w *resp.Writer, args [][]byte) error {
	return s.expireIn(w, args, time.Second)
}

// pexpire: EXPIRE with millisecond resolution.
//
// EXPIRE's second-resolution API cannot express a sub-second deadline, which
// makes both the tests and any real cache with short-lived entries awkward. The
// store works in time.Duration throughout, so the millisecond variants are the
// same handler with a different unit.
func (s *Server) pexpire(w *resp.Writer, args [][]byte) error {
	return s.expireIn(w, args, time.Millisecond)
}

// expireat: EXPIREAT key unix-seconds. The absolute forms are what the
// append-only file carries, because "expires in 60 seconds" is not a fact that
// survives being written down and read back an hour later.
func (s *Server) expireat(w *resp.Writer, args [][]byte) error {
	return s.expireAtIn(w, args, time.Second)
}

func (s *Server) pexpireat(w *resp.Writer, args [][]byte) error {
	return s.expireAtIn(w, args, time.Millisecond)
}

func (s *Server) expireAtIn(w *resp.Writer, args [][]byte, unit time.Duration) error {
	n, ok := parseTTL(args[1], unit)
	if !ok {
		return w.WriteError("ERR value is not an integer or out of range")
	}
	if s.store.ExpireAt(string(args[0]), epochPlus(n)) {
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

// epochPlus turns a since-the-epoch offset into an absolute time.
func epochPlus(d time.Duration) time.Time {
	return time.UnixMilli(0).Add(d)
}

func (s *Server) expireIn(w *resp.Writer, args [][]byte, unit time.Duration) error {
	ttl, ok := parseTTL(args[1], unit)
	if !ok {
		return w.WriteError("ERR value is not an integer or out of range")
	}
	// Unlike SET, a non-positive TTL here is legal and means "delete it now".
	// EXPIRE k -1 is a documented way to remove a key, and unlike SET there is
	// no value being thrown away, so nothing is silently lost.
	if s.store.Expire(string(args[0]), ttl) {
		return w.WriteInteger(1)
	}
	return w.WriteInteger(0)
}

// ttl: TTL key -> seconds remaining, -1 if the key has no deadline, -2 if the
// key does not exist.
//
// Overloading one integer reply with two sentinel values is not a design I
// would choose, but it is the protocol, and the distinction matters: -1 and -2
// are the difference between "cached forever" and "cache miss".
func (s *Server) ttl(w *resp.Writer, args [][]byte) error {
	return s.ttlIn(w, args, time.Second)
}

func (s *Server) pttl(w *resp.Writer, args [][]byte) error {
	return s.ttlIn(w, args, time.Millisecond)
}

func (s *Server) ttlIn(w *resp.Writer, args [][]byte, unit time.Duration) error {
	d, state := s.store.TTL(string(args[0]))
	switch state {
	case store.KeyMissing:
		return w.WriteInteger(-2)
	case store.KeyPersistent:
		return w.WriteInteger(-1)
	default:
		// Round to nearest rather than truncating, as Redis does: a key set
		// with EX 10 and read back a microsecond later should say 10, not 9.
		// The floor of 1 keeps a key that is alive from reporting 0, which a
		// client would read as "expiring this instant".
		n := int64((d + unit/2) / unit)
		if n < 1 {
			n = 1
		}
		return w.WriteInteger(n)
	}
}

// parseTTL converts a client-supplied count of units into a Duration, reporting
// failure for anything that is not an integer or that would overflow.
//
// The overflow check is the point: time.Duration is an int64 count of
// nanoseconds, so EXPIRE k 9999999999999999999 would otherwise wrap around to a
// deadline in the past and delete the key the client just asked to keep.
func parseTTL(arg []byte, unit time.Duration) (time.Duration, bool) {
	n, err := strconv.ParseInt(string(arg), 10, 64)
	if err != nil {
		return 0, false
	}
	if n > int64(math.MaxInt64/unit) || n < int64(math.MinInt64/unit) {
		return 0, false
	}
	return time.Duration(n) * unit, true
}
