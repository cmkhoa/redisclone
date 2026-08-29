# PLANNING.md — Redis-compatible KV store in Go

Project plan and working notes. Captures the ground rules, milestones, and the
current milestone's plan (M4).

## Project goal

Build a Redis-compatible in-memory key-value store in Go as a systems
programming project, understood deeply enough to defend every design decision
in a technical interview.

## Constraints

- Go, standard library only. No third-party dependencies for the server.
- Wire-compatible with real Redis: `redis-cli` must connect and work.
- Every milestone ends in a working, committable, benchmarkable state.

## Milestones

- **M0** — TCP server + RESP protocol parser. PING/ECHO. Correct RESP
  encode/decode for simple strings, errors, integers, bulk strings, arrays.
  ← **done**
- **M1** — GET/SET/DEL/EXISTS with a concurrency-safe store; many simultaneous
  clients handled correctly. ← **done**
- **M2** — Expiration: EXPIRE, TTL, SET with EX. Lazy expiration on access plus
  active background sampling. ← **done**
- **M3** — Durability: append-only file, replay on startup, configurable fsync
  policy. ← **done**
- **M4** — Memory bounds: maxmemory limit with LRU eviction. ← **done**
- **M5** — Benchmark vs real Redis with redis-benchmark on identical hardware;
  profile and find the top bottleneck. ← **done**

Rule: no milestone starts until I declare the previous one working.

## Standing deliverable

**DECISIONS.md** — after each milestone, record what I chose, what I rejected,
and why. The assistant prompts for it at the end of every milestone. This is
the interview-prep artifact.

---

# M4 plan (complete)

## What landed

```
internal/store/evict.go — accounting, policy parsing, sampled LRU eviction
internal/store/store.go — per-entry access clock, counters, bounded memory
internal/server/         — OOM gate, DBSIZE and INFO
cmd/redisclone/          — -maxmemory and -maxmemory-policy startup flags
```

- `-maxmemory` accepts raw bytes and `k`/`kb`, `m`/`mb`, `g`/`gb` units; zero
  remains unlimited. Policies are `noeviction` (default), `allkeys-lru`,
  `allkeys-random`, and `volatile-lru`.
- Memory is an explicit key/value-plus-per-entry estimate, not process RSS.
  It is kept accurate across overwrites, deletes, lazy expiration, active
  expiration, and eviction.
- LRU uses a coarse logical clock refreshed by the existing expiry ticker and
  evicts the oldest of five sampled candidates. Reads stay under an RLock;
  recency itself is an atomic field.
- The command dispatcher enforces the previous command's overage before each
  write. This permits at most one write's worth of overshoot and lets
  `noeviction` reject the next mutation with `-OOM` unchanged.
- `INFO` exposes memory, eviction, expiry, hit/miss, persistence, and keyspace
  counters. Eviction emits a journalled `DEL`, so replay preserves the
  post-eviction keyspace.

## Verification

- `make test`, `go test -race ./internal/... ./cmd/...`, `go vet ./...`, and
  `make e2e` pass.
- Store tests cover accounting, policies, LRU versus random under locality,
  journal effects, and concurrent access under the race detector.
- M4 e2e exercises unlimited mode, OOM refusal, allkeys/volatile LRU,
  background expiry metrics, and AOF replay after eviction.

---

# M5 plan (complete)

## Method and results

Measured on 2026-08-29 on an Apple M1 Pro (darwin/arm64, Go 1.26.1), with
both servers bound to loopback, persistence disabled, 50 clients, pipeline
depth 16, and one million requests each for SET and GET:

| Server | SET requests/s | GET requests/s |
| --- | ---: | ---: |
| redisclone | 482,625 | 1,237,624 |
| Redis | 1,162,791 | 1,424,501 |

The comparison used `/opt/homebrew/bin/redis-benchmark -t set,get -n 1000000
-c 50 -P 16 -q` against each server on the same machine. `redisclone` prints
a harmless CONFIG warning because CONFIG is intentionally unimplemented.

An opt-in `-pprof-addr 127.0.0.1:6060` flag exposes standard Go profiling
endpoints. A synchronized ten-second SET profile under the same concurrent
load found `Store.set` on the single keyspace mutex to be the decisive
write-path cost: mutex lock/unlock was ~36% cumulative CPU. Store microbenchmarks
corroborate the contention: parallel SET rises from 128 ns/op at one CPU to
375 ns/op at eight; GET rises from 53 ns/op to 224 ns/op.

No sharding was applied in M5. It would improve contention, but needs an
explicit cross-shard atomicity design for DEL and a new correctness benchmark;
it is larger than a measurement-only milestone. The result is a clear next
optimization target rather than an unmeasured rewrite.

---

# M3 plan (complete)

## What landed

```
internal/aof/aof.go     — the log: Append (buffer only), Sync, policies
internal/aof/replay.go  — Replay, torn-tail repair, corruption refusal
internal/store/store.go — journals effects under the keyspace lock
internal/server/        — replay via the real handlers, -MISCONF gate
```

- Flags: `-appendonly`, `-appendfsync always|everysec|no`, `-appendfilename`,
  `-dir`. Durability is off by default.
- The log is a stream of RESP arrays — same encoder and parser as the wire,
  and `redis-cli --pipe` can read it.
- **Effects, not commands:** `SET k v EX 60` is logged as
  `SET k v PXAT <absolute-ms>`, `DEL` logs only the keys that existed, and
  expiry collection logs nothing (the deadline is already recorded).
- **Ordering:** the append happens *under* the keyspace write lock, so the
  log's order is the mutation order. The fsync happens outside it, because an
  fsync under the keyspace lock would stall every client.
- New commands: `EXPIREAT`, `PEXPIREAT`, and `SET ... EXAT|PXAT` — the log's
  vocabulary is all things a client could type.
- A failed log latches its error; write commands then reply `-MISCONF` while
  reads keep working.

## Verification

- `make test` / `-race` — AOF format, torn tails at four different cut points,
  corruption refusal, `EncodedLen` matching reality, latched write errors.
- The ordering property is **mutation-tested**: moving the append outside the
  keyspace lock makes `TestJournalOrderMatchesMutationOrder` fail.
- `make e2e` — `test/e2e/m3_test.go` starts *real server processes* and kills
  them: data survives a restart, deadlines keep ticking across one, `always`
  survives `kill -9`, a torn tail is repaired and still appendable, a corrupt
  log refuses to start.
- Measured, not assumed: `always` 777 rps / p50 53.7 ms, `everysec` 61,920 rps,
  `no` 88,496 rps, AOF off 89,286 rps. `kill -9` after 20 writes leaves 0 bytes
  under `everysec` and all of them under `always`.

## Open decisions for DECISIONS.md (settled, with reasoning recorded)

1. Log effects rather than commands; absolute deadlines; one record per SET.
2. Append under the keyspace lock, fsync outside it.
3. fsync policy trade-offs, with numbers and the macOS `F_FULLFSYNC` caveat.
4. Torn tail repaired, corruption refused.
5. Replay through the real handlers, plus the validation that adds.
6. `-MISCONF` on a failed log — and the one write that still slips through.

## Exit criteria for M3

- `make test`, `-race`, `make e2e` green.
- `internal/aof` reviewed.
- M3 section of DECISIONS.md filled in.
- Then, and only then, M4.

## Known gap carried into M4

No `BGREWRITEAOF`: the log grows without bound. The rewrite needs a consistent
snapshot of the keyspace to walk, which interacts with M4's eviction — doing it
before then would mean doing it twice.

---

# M2 plan (done)

## What landed

```
internal/store/expire.go — the sampling cycle and its ticker
internal/store/store.go  — entries carry a deadline; lazy collection on read
internal/server/commands.go — EXPIRE, PEXPIRE, TTL, PTTL, SET ... EX/PX
```

- Entries are now `{val, expiresAt}`, with a second map (`expiring`) indexing
  just the volatile keys so the sampler has something small to sample.
- **Lazy expiration** in `Get`: an expired key reports missing and is deleted.
  The delete needs the write lock, which means dropping the read lock — so the
  deadline is re-checked under the write lock before anything is removed.
  Without that re-check a concurrent `SET` gets silently thrown away.
- **Active expiration** in `RunActiveExpiration`: every 100 ms, sample 20
  volatile keys, delete the expired ones, repeat immediately while ≥25% of a
  sample is expired (max 16 rounds). Started by `Server.StartBackgroundTasks`,
  stopped by cancelling its context.
- Commands: `SET k v [EX s | PX ms]`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`.
  `TTL` returns -2 missing / -1 persistent / rounded seconds otherwise.
  Millisecond variants were added beyond the plan — second resolution can't
  express a sub-second deadline, and every timing test would need whole-second
  sleeps.

## Verification

- `make test` / `-race` — store tests plant already-expired entries directly
  (same-package tests), so nothing waits on the clock except the two tests
  that are specifically about waiting. `checkIndex` asserts the two maps agree
  after a concurrent hammer.
- The re-check race is driven deterministically through a test hook, and was
  **verified by mutation**: with the re-check removed, the test fails. A racing
  version of the same test passed against the broken code every run.
- `make e2e` — `test/e2e/m2_test.go`: TTL sentinels, keys actually vanishing,
  expiry visible across connections, every error case.
- `redis-cli -p 6379 set session abc ex 100` / `ttl` / `pttl` / `expire`.

## Open decisions for DECISIONS.md (settled, with reasoning recorded)

1. Why both lazy *and* active expiration — which one owns correctness, which
   owns memory.
2. Sampling vs a timer per key vs a min-heap of deadlines.
3. The volatile-key index: why sampling the main keyspace degenerates.
4. The read-lock upgrade window and the re-check.
5. `time.Time` (monotonic, survives clock steps) vs Unix ms — and why M3's AOF
   forces a partial retreat.

## Exit criteria for M2

- `make test`, `-race`, `make e2e` green.
- `internal/store/expire.go` reviewed.
- M2 section of DECISIONS.md filled in.
- Then, and only then, M3.

---

# M1 plan (done)

## What landed

```
internal/store/          — the keyspace: RWMutex + map[string][]byte
internal/server/commands.go — dispatch table + PING/ECHO/SET/GET/DEL/EXISTS
```

- `internal/store` — `Get`, `Set`, `Del(keys...)`, `Exists(keys...)`, `Len`.
  One `sync.RWMutex` over one map. Values are never copied and never mutated in
  place; that invariant is what lets `Get` return the live slice with no lock
  held. Documented at the top of the package because the first in-place
  mutation (APPEND, SETRANGE) will break it if nobody reads it.
- `internal/server` — M0's `switch` became a `map[string]command` table with
  `minArgs`/`maxArgs`, so arity is checked in exactly one place.
- Replies: `SET` → `+OK`; `GET` → bulk or `$-1`; `DEL`/`EXISTS` → integer.
  `EXISTS k k` is 2, `DEL k k` is 1 — matching real Redis.
- `SET k v EX 10` is an arity error until M2 can honour the expiry.

## Verification

- `make test` — store unit tests (including a 16-goroutine hammer that fails
  under `-race` if the map is ever touched unlocked); server command tests over
  `net.Pipe`.
- `make e2e` — `test/e2e/m1_test.go`: shared keyspace across connections,
  50 concurrent clients on private keys, 25 clients contending on one key,
  binary-safe keys *and* values, 1 MiB values, arity errors.
- `go test ./internal/store -bench . -benchmem -cpu 1,4,8` — the numbers behind
  the locking decision; see DECISIONS.md.
- `redis-cli -p 6379 set greeting "hello world"` / `get` / `exists` / `del`.

## Open decisions for DECISIONS.md (settled, with reasoning recorded)

1. Lock granularity: single RWMutex vs sharded vs `sync.Map` — benchmarked all
   three, kept the simple one, wrote down when to revisit.
2. Value ownership: no copy on `Set` or `Get`, backed by a documented
   no-in-place-mutation invariant.
3. Dispatch: table with arity metadata, not a switch.
4. Multi-key `DEL`/`EXISTS`: one lock acquisition for the batch, so the command
   is atomic against other clients.

## Exit criteria for M1

- `make test`, `make test -race`, `make e2e` green.
- `internal/store` reviewed.
- M1 section of DECISIONS.md filled in.
- Then, and only then, M2.

---

# M0 plan (done)

## Scaffolding in place (assistant-written)

- `go.mod` (module `redisclone`, go 1.26)
- `Makefile` — build / run / test / e2e / fmt / vet
- `test/e2e/m0_test.go` — black-box harness: builds the binary, boots it on a
  free port, speaks raw RESP bytes over TCP. Contains no RESP parser (exact
  byte comparisons + `-ERR` prefix checks only). Assumes the binary accepts
  `-addr host:port` (default `:6379`).
- `DECISIONS.md` skeleton with per-milestone prompts.

## RESP2 essentials

Every value: one type byte, CRLF-terminated (`\r\n`, always both bytes).

| Type | Byte | Example | Notes |
|---|---|---|---|
| Simple string | `+` | `+OK\r\n` | no CRLF inside |
| Error | `-` | `-ERR unknown command 'FOO'\r\n` | |
| Integer | `:` | `:1000\r\n` | int64 |
| Bulk string | `$` | `$5\r\nhello\r\n` | length-prefixed, binary-safe |
| Array | `*` | `*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n` | count-prefixed |

Specials: `$-1\r\n` null bulk string, `*-1\r\n` null array, `$0\r\n\r\n` empty
bulk string.

Key asymmetry:
- Requests are (almost) always an array of bulk strings → request parser only
  needs `*` containing `$`.
- Replies use all five types → encoder needs all five.

Binary safety: bulk payloads may contain `\r\n` / NUL — read the `$N` header
line, then exactly N bytes via `io.ReadFull`, then consume and verify the
trailing CRLF. Never read the payload line-by-line (most common M0 bug; the
harness tests for it).

Compatibility notes:
- Modern redis-cli sends `HELLO 3` / `COMMAND DOCS` on connect. Replying
  `-ERR unknown command ...` is fine — it degrades to RESP2. Don't crash on
  unknown commands.
- "Inline commands" (bare `PING\r\n` from netcat) are optional.

Spec: https://redis.io/docs/latest/develop/reference/protocol-spec/ (RESP2 half).

## Server structure: goroutine-per-connection

```
listener := net.Listen("tcp", addr)
accept loop: conn := Accept(); go handleConn(conn)

handleConn:
  defer conn.Close()
  bufio.Reader + bufio.Writer around conn
  loop: parse one command → dispatch → write reply → Flush
  io.EOF → clean disconnect, return
```

Interview material: real Redis is a single-threaded epoll/kqueue event loop —
atomic command execution for free, but one slow command stalls everyone. Go's
runtime multiplexes goroutines onto OS threads (a blocked goroutine costs ~KBs
of stack, not an OS thread), so goroutine-per-conn scales to tens of thousands
of connections with straight-line handler code. Cost accepted: commands run
concurrently, so M1's store must be made safe explicitly.

Buffering: `net.Conn.Read` returns arbitrary byte-stream fragments (half a
command or three commands). `bufio.Reader` handles reassembly; `bufio.Writer`
batches replies — remember `Flush()` after replying (forgetting it looks like
a hung server; second most common M0 bug).

## Stdlib APIs

- `net.Listen` / `Accept` / `net.Conn`
- `bufio.NewReader` / `NewWriter`; `ReadString('\n')` keeps the delimiter —
  strip and verify `\r\n` manually; `io.ReadFull` for payloads
- `strconv.Atoi` / `ParseInt` for lengths (reject garbage; only -1 negative)
- `strings.ToUpper` — command names are case-insensitive
- `errors.Is(err, io.EOF)`
- `flag` — binary must accept `-addr host:port` (default `:6379`)

## Implementation checklist (my code)

Layout the Makefile assumes:

```
cmd/redisclone/main.go   — flags, listen/accept loop
internal/resp/           — RESP reader + writer + unit tests
internal/server/         — connection handler + dispatch
```

1. RESP **writer** first (easier): encode all five types onto an `io.Writer`;
   unit-test against hand-written byte strings.
2. RESP **reader**: parse one command (array of bulk strings). Handle: non-`*`
   first byte, bad integers, negative lengths, missing trailing CRLF, EOF
   mid-command. Unit-test with `strings.NewReader` — no sockets.
3. **Server**: dispatch on `strings.ToUpper(args[0])`:
   - `PING` → `+PONG\r\n`; `PING msg` → bulk `msg`; else arity error
   - `ECHO msg` → bulk `msg`; else arity error
   - unknown → `-ERR unknown command '<name>'`
4. **Error policy**: command errors (arity/unknown) reply `-ERR`, connection
   stays alive. Protocol errors (malformed RESP) may reply then close — byte
   stream position is no longer trustworthy.
5. Defensive limits: cap bulk length (Redis uses 512MB) and array count;
   reject before allocating.

## Verification

- `make e2e` — full black-box suite (pipelining, fragmented writes, binary
  safety, 1MiB payload, 50 concurrent clients, malformed input, arity errors)
- `redis-cli -p 6379 ping`, `redis-cli ECHO "hello world"`
- `printf '*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n' | nc localhost 6379`

## Open decisions for DECISIONS.md (mine to make)

1. Parser return type: `[]string` (convenient) vs `[][]byte` (no per-arg copy,
   honest about binary data, but aliasing/reuse hazards).
2. Where protocol limits live and what a violation does: error-and-close vs
   error-and-resync.
3. Inline command support: yes/no.
4. Reply writing: encode directly onto `bufio.Writer` vs build reply in a
   `[]byte` first (consider pipelining and future `MULTI`).

## Exit criteria for M0

- `make e2e` green; `redis-cli ping` works.
- `internal/resp` reviewed.
- M0 section of DECISIONS.md filled in.
- Then, and only then, M1.
