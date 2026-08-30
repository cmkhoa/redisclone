# Design Decisions

One section per milestone. For each significant choice: what I chose, what I
rejected, and why. Written for interview prep — every entry should survive a
"why didn't you do it the other way?" follow-up.

## M0 — TCP server + RESP parser

### Concurrency model: goroutine-per-connection

**Chosen:** `Accept()` in a loop, `go HandleConn(conn)` per client
([server.go](internal/server/server.go)).

**Rejected:** a single-threaded event loop over epoll/kqueue, which is what
real Redis does.

**Why:** Redis's event loop buys atomic command execution for free — there is
only one thread, so no command can interleave with another, and no data
structure in the codebase needs a lock. The price is that one slow command
(`KEYS *` on a big keyspace) stalls every other client. Go inverts the
trade-off: the runtime already multiplexes goroutines onto epoll/kqueue
underneath, so a goroutine blocked on a socket read costs a few KiB of stack
rather than an OS thread, and tens of thousands of connections is fine. What we
get for that is handler code written as straight-line blocking reads and
writes, which is dramatically simpler than state machines driven by readiness
callbacks. What we pay is the atomicity: commands from different connections
really do run concurrently, so M1's keyspace has to be made safe explicitly
rather than by construction.

**Follow-up to expect:** "so you've traded a correctness guarantee for
convenience?" — yes, deliberately, and the cost is confined to one place (the
store). Redis's own answer to multi-core has been to shard processes; the
event loop doesn't scale past one core either.

### Parser output: `[][]byte` with copied payloads

**Chosen:** `ReadCommand() ([][]byte, error)`, where each argument is a freshly
allocated slice the caller owns.

**Rejected:** `[]string` (convenient, but every argument is a copy anyway *and*
a lie about the data — RESP bulk strings are arbitrary bytes, not UTF-8), and
`[][]byte` aliasing the read buffer (zero-copy, but every argument becomes
invalid on the next `ReadCommand`, which is a use-after-free waiting to happen
the moment M1 stores a value).

**Why:** the aliasing version saves one copy per argument on the read path and
then makes the store's job impossible — `SET k v` has to keep `v`. Paying an
allocation per argument at the parse boundary buys an ownership rule with no
exceptions: what the parser returns is yours. `TestReadCommandArgsDoNotAliasBuffer`
pins that down. If profiling in M5 says these allocations matter, the fix is an
arena or a per-connection scratch buffer with an explicit lifetime, not a
silent aliasing rule.

### Buffering, and where the flush goes

**Chosen:** `bufio.Reader` (16 KiB) and `bufio.Writer` (16 KiB) per connection.
No explicit flush after each reply — instead the reader reads through a
`flushBeforeRead` wrapper that flushes the write buffer before touching the
socket.

**Rejected:** flush after every command (correct but one syscall per reply,
which throws away most of pipelining's benefit), and flush only when the read
buffer is empty (`r.Buffered() == 0`), which deadlocks if a client sends one
complete command plus the first bytes of another and then waits for a reply.

**Why:** `bufio.Reader` only calls `Read` on the socket when its buffer runs
dry, and that is precisely the moment we are about to block waiting for the
client — the last possible instant at which a pending reply must be on the
wire. Hooking the flush there is exactly the "flush before you block" rule,
expressed once, in the one place that can't be forgotten. A pipelined batch of
N commands coalesces into a single `write` automatically; a request/response
client can never hang. `net.Conn.Read` hands back arbitrary fragments of the
byte stream (half a command, or three of them), which is what the
`bufio.Reader` is there to reassemble.

**Follow-up to expect:** "what if a handler wants to force a flush?" — nothing
in M0 does. When something does (a blocking command in a later milestone), it
calls `Flush` before it blocks, for the same reason.

### Error policy: command errors reply, protocol errors reply and close

**Chosen:** bad arity and unknown commands are `-ERR` replies and the
connection continues. Malformed RESP is a `*resp.ProtocolError`: reply
`-ERR Protocol error: ...`, then close.

**Rejected:** resynchronising after a protocol error (skip to the next `*`).

**Why:** a command error is a normal exchange — the request parsed fine, we
just won't run it. A protocol error is categorically different: we no longer
know where the next command starts. `$100\r\n` with a wrong length makes every
following byte ambiguous, and "skip to the next `*`" is a guess that can
resynchronise onto a byte *inside* a payload and execute attacker-chosen bytes
as commands. Closing is the only honest option, and it's what Redis does. The
type distinction (`ProtocolError` vs an `io` error vs a plain reply) is what
makes the policy readable in one `switch` rather than scattered through the
parser.

Two related sub-decisions:
- **Limits are the parser's job**, not the handler's: `MaxBulkLength` (512 MiB,
  as Redis) and `MaxArrayLength` (1 M) are checked against the header number
  *before* anything is allocated from it, so `$536870913\r\n` is a protocol
  error and not an OOM. A header line that doesn't fit the 16 KiB read buffer
  is likewise an error rather than unbounded buffering.
- **Anything from the wire that goes into an error message is sanitised**
  (`server.safeName`). Error replies are CRLF-terminated with no length prefix,
  so echoing a command name back verbatim would let a client inject
  `\r\n+PONG\r\n` and desynchronise its *own* stream — harmless here, but the
  same shape as a response-splitting bug, and free to prevent.

### Inline commands: not supported

**Chosen:** any first byte other than `*` is a protocol error.

**Why:** inline commands (`PING\r\n` typed into netcat) exist for humans with a
telnet client. No real client library sends them, they need a second parser
with its own quoting rules, and every byte of that parser is attack surface
that nothing in the test suite or `redis-cli` exercises. `make e2e`'s
`TestMalformedInputGetsErrorNotHang` explicitly permits either behaviour.
Revisit only if hand-debugging over `nc` becomes a real workflow.

### Reply encoding: header + elements, straight onto the buffer

**Chosen:** `WriteArrayHeader(n)` followed by n element writes, encoded
directly onto the `bufio.Writer`. No intermediate reply object.

**Rejected:** building each reply as a `[]byte` (or a `[]any` tree) and writing
it in one go.

**Why:** the encoder never has to know the set of possible value types, and a
large reply streams out through the 16 KiB buffer instead of being
materialised in full first. Pipelining is already handled a layer down by the
buffer, so an intermediate buffer would add a copy and buy nothing. The
`Writer` keeps a 24-byte scratch array as a field (not a local) purely because
passing a local array to an `io.Writer` interface method forces it to escape —
that's the difference between one allocation per reply and zero, pinned by
`TestWriteAllocations`.

**Where this could bite:** `MULTI`/`EXEC` needs a reply whose array header
count isn't known until the last queued command has run. Since commands are
queued, not their replies, the count *is* known at `EXEC` time — the header
still comes first. A genuinely streaming, unknown-length reply would need
RESP3's `*?`-style aggregates or an intermediate buffer; neither is a RESP2
concern.

### Simple strings and errors panic on CR/LF

`WriteSimpleString` and `WriteError` panic if handed a newline, rather than
returning an error. They have no length prefix, so a newline inside one
silently corrupts the stream. Both are only ever called with program-authored
constants — a newline in one is a bug in *this* program, not bad input from the
network, and a panic in a handler goroutine is caught and logged per connection
without taking the server down. Client data goes in bulk strings, which are
length-prefixed and binary-safe by construction.

## M1 — Concurrent store (GET/SET/DEL/EXISTS)

### One RWMutex over one map

**Chosen:** `sync.RWMutex` guarding a single `map[string][]byte`
([store.go](internal/store/store.go)).

**Rejected:** a sharded map (N maps, N locks, key hashed to a shard),
`sync.Map`, and a plain `sync.Mutex`.

**Why, with numbers.** I benchmarked all three shapes at `-cpu 8` on an M1 Pro
(4096 keys, parallel):

| | read-only | 9 reads : 1 write | write-only |
|---|---|---|---|
| `sync.Mutex` | 199 ns/op | 208 ns/op | 299 ns/op |
| `sync.RWMutex` | 155 ns/op | 71 ns/op | 293 ns/op |
| 16 shards × RWMutex | 35 ns/op | 63 ns/op | 106 ns/op |

Three things fall out of that table:

1. **RWMutex beats Mutex on anything read-heavy** — 3× on the realistic 9:1
   mix — which settles that choice. Reads genuinely run in parallel; the
   remaining cost is that `RLock` still writes to one shared cache line, so
   readers ping-pong it between cores. That is why `BenchmarkGetParallel` in
   the store's own suite gets *slower* per op as `-cpu` rises (20 ns at 1 core,
   160 ns at 8): the map lookup is not the cost, the lock's cache line is.
2. **Sharding is a real 2–4× win** and it is not subtle. I am still not taking
   it in M1: the single lock's worst case is ~6.4 M ops/s, and a single TCP
   connection doing request/response is nowhere near that — the syscall per
   round trip is orders of magnitude more expensive. Optimising the part that
   isn't the bottleneck, before M5 has measured where the bottleneck is, is the
   mistake this milestone plan exists to avoid.
3. `sync.Map` never came into it: it is tuned for caches with stable keys read
   far more than written, and it costs an `any` box per value. A keyspace is
   the opposite workload.

**Follow-up to expect:** "when would you shard?" — when M5's profile shows lock
contention above the syscall cost, i.e. once batching or `io_uring`-style
amortisation has made the network cheap enough for the lock to matter. The
`Store` API (Get/Set/Del/Exists on whole batches) is deliberately shaped so
sharding is an internal change with no caller impact; the one thing it would
cost is cross-shard atomicity for multi-key `DEL`, which is exactly the
argument real Redis Cluster has with `MGET`.

### Values are never copied, and never mutated in place

**Chosen:** `Set` takes ownership of the caller's slice; `Get` returns the live
stored slice. No defensive copy on either side. The invariant that makes it
safe is written at the top of the package: *a value in the store is never
mutated in place.*

**Rejected:** copying on `Set` (safe against a caller that keeps writing to the
slice), or copying on `Get` (safe against a caller that writes to the reply).

**Why:** the parser already allocates a fresh slice per argument (M0's decision),
so a copy on `Set` would be the *second* copy of every value written, on the
hot path, to defend against a caller that does not exist. The invariant does
the same job for free: an overwrite replaces the map entry with a different
slice rather than writing over the old bytes, so a reader holding a slice from
before the overwrite keeps reading valid — if stale — data, with no lock held.
`TestReadValueSurvivesOverwriteAndDelete` pins that down.

**Where this bites:** the first command that wants to modify a value in place —
`APPEND`, `SETRANGE`, `INCR` on a big number — must copy-on-write instead of
appending into the stored slice, or it will mutate bytes another goroutine is
in the middle of writing to a socket. That is a real trap and the reason the
invariant is documented at the package level rather than in a comment on `Set`.

### A dispatch table, not a switch

**Chosen:** `map[string]command`, where `command` carries `minArgs`, `maxArgs`
and the handler ([commands.go](internal/server/commands.go)). Arity is checked
once, in `dispatch`, before the handler runs.

**Rejected:** M0's `switch` with each case checking its own argument count.

**Why:** with two commands the switch was fine; the moment SET/GET/DEL/EXISTS
arrived, "did this handler remember to check its arity?" became a question
asked six times. Now every handler indexes the arguments it was promised, and a
new command physically cannot forget. The table also makes "does this command
exist, and what is its arity" answerable *without running the handler*, which
is what `COMMAND DOCS` and `MULTI`'s queue-time validation both need later.

Redis encodes arity as one signed int (negative = "at least |n|"), which cannot
express PING's "zero or one". Two fields cost nothing and say what they mean.

### DEL and EXISTS count duplicates differently

`EXISTS k k k` on an existing key is `3`; `DEL k k` is `1`. This looks like a
bug and is what real Redis does: `EXISTS` answers once per argument, `DEL`
reports how many keys it actually removed — and a key can only be removed once.
Matching the real behaviour matters more than internal consistency here,
because clients and test suites depend on it.

### Multi-key commands take one lock, not one per key

`Del(keys ...string)` and `Exists(keys ...string)` take the whole batch and
acquire the lock once. Cheaper, and more importantly it makes a multi-key `DEL`
atomic with respect to other commands — no other client can observe a state
where half the batch is deleted. Doing it key-by-key would be the kind of
"obviously equivalent" refactor that quietly breaks a guarantee clients rely
on.

### `SET key value` only — options are an arity error

`SET k v EX 10` replies `-ERR wrong number of arguments` today rather than
setting the key and ignoring the expiry. Accepting an option and silently
dropping it is the worse failure: the client believes the key will disappear
and it never does, which surfaces as a data leak or a stale-cache bug far from
its cause. EX/NX/XX arrive with M2, when the store can actually honour them.

## M2 — Expiration

### Two enforcement paths, because neither works alone

**Chosen:** lazy expiration on every read *and* a background sampler
([store.go](internal/store/store.go), [expire.go](internal/store/expire.go)).

**Why both.** They solve different problems and each is useless at the other's
job:

- **Lazy alone is correct but unbounded.** Every read filters expired keys, so
  no client can ever see one — but a key written with a TTL and never read again
  holds its memory forever. A cache whose expired entries are only freed when
  someone asks for them is not a cache with a memory bound.
- **Active alone is bounded but incorrect.** Between a key's deadline and the
  sampler reaching it there is a window, and a read landing in that window would
  return data that should be gone.

So: the lazy path owns *correctness*, the sampler owns *memory*. Stating which
one owns what is the thing to have straight — it is why the sampler is allowed
to be approximate, and why the lazy path is not.

### The sampling algorithm, and the two designs I didn't take

**Chosen:** Redis's adaptive sampling. Every 100 ms, take 20 random volatile
keys, delete the expired ones, and if a quarter or more of the sample was
expired, immediately go round again — up to 16 rounds. Constants in
[expire.go](internal/store/expire.go).

The feedback loop is the clever part, and it's worth being able to explain: the
hit rate in a random sample estimates the proportion of expired keys in the
whole keyspace. High hit rate means there is a backlog worth clearing now; low
means there isn't, so stop and give the CPU back. Work per cycle is therefore
proportional to work *available*, not to keyspace size — and both the constant
sample and the round cap keep any single cycle short, so the write lock is
never held long enough for a client to notice.

**Rejected: a timer per volatile key** (`time.AfterFunc` on every `SET ... EX`).
Precise, and superficially the obvious Go answer. It costs a runtime timer per
volatile key — heap insert on every write, and cancel-and-replace on every
overwrite of a key that already had a TTL. A million volatile keys is a million
timers in one global heap, all of which fire in a burst if a batch was written
together. It trades a bounded background cost for an unbounded foreground one,
on the hot path.

**Rejected: a min-heap of deadlines**, popped by one goroutine that sleeps until
the next one. This is the genuinely defensible alternative and I want to be
honest about it: it gives exact expiry with no sampling error and O(log n)
writes. What it costs is a heap operation on every `SET ... EX` (versus a map
insert), plus the awkwardness that overwriting a key's TTL leaves a stale heap
entry — you either pay to find and remove it, or you tolerate lazy tombstones
and re-check on pop, which is its own bookkeeping. Redis's sampler is
approximate but pushes *all* its cost to the background, and expiry precision
is not something clients can observe anyway, because the lazy path already
hides expired keys. Given that, paying on every write for precision nobody can
see is the wrong trade.

**Follow-up to expect:** "what's the worst case for sampling?" — a keyspace
where a tiny fraction of volatile keys are expired: the hit rate stays below
25%, so each cycle does exactly one round of 20 and those keys linger. That is
the intended behaviour (they are invisible either way, and memory pressure is
low by construction), but it is the reason M4's eviction cannot rely on the
sampler having already cleaned up.

### A second map for volatile keys

**Chosen:** `expiring map[string]struct{}` alongside the keyspace, holding
exactly the keys that have a deadline.

**Rejected:** sampling the main keyspace directly.

**Why:** sampling only works if samples can plausibly be expired. With a million
persistent keys and ten volatile ones, twenty random samples from the keyspace
almost never touch a key that *can* expire, and the sampler degenerates into a
CPU burner that finds nothing. Restricting the sample to volatile keys makes
the algorithm's hit rate mean what it is supposed to mean.
`TestActiveExpireCycleOnlySamplesVolatileKeys` pins it: 10,000 persistent keys
and one expired needle, and the cycle scans exactly one key.

**The cost is real:** two maps that six methods have to keep consistent, which
is a class of bug that shows up as a slow leak rather than a crash. Mitigations:
every deletion goes through `deleteLocked`, and `checkIndex` asserts the
invariant (`in the index ⟺ has a deadline`) after a concurrent hammer test.

### The read-lock upgrade problem

`Get` finds an expired key under `RLock`, but deleting needs `Lock`, and Go's
`RWMutex` has no upgrade (it couldn't have one — two readers upgrading at once
is a guaranteed deadlock). So `Get` drops the read lock, takes the write lock,
and **re-checks the deadline against a fresh `now`** before deleting.

That re-check is not defensive programming, it is load-bearing: in the window
between the two locks another client can `SET` the key afresh, and without the
re-check `Get` deletes a value that is very much alive — silent data loss under
exactly the load that makes it hard to reproduce.

Testing it honestly took a second try. A version racing two goroutines passed
against deliberately broken code every time, because the window is a few hundred
nanoseconds wide. The test now drives the window through a hook
(`testHookExpiredWindow`, nil in production, on the already-slow path only), and
I verified it by mutation: with the re-check removed, it fails. **A concurrency
test that has never been shown to fail against the bug it describes is
decoration.**

`TTL` deliberately does *not* collect expired keys, for the same family of
reasons in reverse: it would turn a pure read into a write-lock acquisition on
every call, to do cleanup the sampler does anyway. `Get` collects only because
it has to take the write lock regardless.

### Deadlines as `time.Time`, not Unix milliseconds

**Chosen:** `expiresAt time.Time`, zero value meaning "no deadline".

**Why:** `time.Now()` carries a monotonic reading, and comparing two such Times
uses it — so a deadline is measured against elapsed time, not against a wall
clock that NTP or an administrator can step. Storing `int64` Unix milliseconds
(what Redis does) is 16 bytes smaller per entry but re-derives every deadline
from a clock that can jump backwards or forwards, which turns a clock
correction into mass premature expiry or mass immortality.

**What it costs, and where it breaks:** 16 bytes per entry, and the monotonic
reading cannot be serialised. M3's AOF has to write an absolute wall-clock
deadline, so replay will reconstruct `time.Time`s *without* a monotonic
reading — meaning restored keys are wall-clock-based whether I like it or not.
That's an argument for revisiting this in M3, not for pre-emptively giving up
the property now.

### Semantics I matched to Redis on purpose

Each of these looks arbitrary and each has a reason:

- **A plain `SET` clears the TTL.** Otherwise overwriting a cached value would
  silently inherit the old entry's deadline. (`KEEPTTL` is the opt-out, and is
  not implemented.)
- **`SET k v EX 0` is an error, but `EXPIRE k -1` deletes the key.** They read
  as inconsistent and aren't: a non-positive expiry on `SET` throws away a value
  the client just supplied, which is nearly always a caller bug worth surfacing;
  `EXPIRE` with a past deadline discards nothing the client didn't already have,
  and is a documented idiom for deleting a key.
- **`TTL` rounds to nearest, with a floor of 1.** A key set with `EX 100` and
  read a microsecond later reports 100, not 99; a key with 10 ms left reports 1,
  not 0, because a client reading 0 would take it as "gone this instant".
- **`-2` for missing, `-1` for no deadline.** Overloading one integer with two
  sentinels is not a design I'd choose, but it is the protocol — and the
  distinction is "cache miss" versus "cached forever", which callers act on
  differently.
- **An unknown `SET` option is `-ERR syntax error`**, never ignored. Same
  argument as M1: a client that asks for `NX` and gets an unconditional
  overwrite is worse off than one that gets an error.

### PEXPIRE and PTTL, beyond the milestone's scope

The plan called for EXPIRE/TTL/SET EX. I added the millisecond variants because
second resolution cannot express a sub-second deadline, which makes both a
real short-TTL cache and every test of expiry-over-time awkward — the e2e suite
would have to sleep for whole seconds. The store works in `time.Duration`
throughout, so each variant is the same handler with a different unit: about
four lines each.

Still absent, deliberately: `PERSIST`, `EXPIREAT`/`PEXPIREAT`, `KEEPTTL`,
`NX`/`XX`, and `RANDOMKEY`.

### What is not observable over the wire

There is no `DBSIZE` or `INFO`, so a black-box test cannot see the sampler
working — it can only see that expired keys are invisible, which the lazy path
alone would satisfy. Active expiration is therefore verified in
`internal/store` (including a real-ticker test that writes 100 keys, reads
none, and waits for the keyspace to empty), and that gap is the reason: it is
not that the e2e suite forgot. `DBSIZE` would close it, and M4 needs a way to
observe memory anyway.

## M3 — AOF persistence

### The log stores effects, not the commands clients sent

**Chosen:** the store journals what it actually did, from inside the mutation
([store.go](internal/store/store.go)) — not what the client asked for.

The three cases where those differ, and each one is a bug avoided:

| Client sent | Log records | Why |
|---|---|---|
| `SET k v EX 60` | `SET k v PXAT 1788041698641` | "in 60 seconds" means something different every time it is replayed |
| `DEL a missing b` | `DEL a b` | replay must not depend on the state the keyspace happened to be in |
| *(key expires)* | *nothing* | the deadline is already in the log; replay reaches the same conclusion itself |

**Why:** replay has to be deterministic and idempotent — the same log applied
twice, a day apart, must produce the same keyspace. A relative deadline breaks
that outright: log `SET k v EX 60`, restart three times over an hour, and the
key is immortal, renewed for another minute on each restart.
`TestRepeatedRestartsDoNotResetDeadlines` is the regression test, and it fails
loudly against the naive design.

Real Redis does the same thing for the same reason (it rewrites relative
expires to `PEXPIREAT` on the way into the AOF). The cost is that the store
knows command names, which is a layering compromise I took deliberately — see
the ordering decision below, which forces the journal to live at that level
anyway.

### The deadline travels with the SET, in one record

`SET k v PXAT <ms>` is one journal record, not `SET k v` followed by
`PEXPIREAT k <ms>`.

Two records would be atomic only if the log could not be torn between them —
and a torn tail is precisely the failure a crash produces. The half-applied
version of the two-record form is *a key with no deadline*: a value that should
have lived 60 seconds becomes permanent, silently, and only after a crash. One
record makes that state unrepresentable. (This is also why `SET`'s `PXAT`/`EXAT`
options exist as real commands here — the log's own vocabulary should be
something a client could have typed.)

### The journal append happens under the durable-commit gate

**Chosen:** `Store.propagate` is called while holding the durable-commit gate;
the append encodes into a buffer and nothing else. The `write(2)` and the
fsync happen later, after that gate is released.

**Rejected:** appending after releasing the gate (the obvious way, and much
easier to keep the store ignorant of command names).

**Why:** the log's order has to *be* the order mutations happened in. Two
clients race to `SET k`; the key's shard lock picks a winner, and the durable
gate commits its record in the same order. If the append happens after the gate
is released, the loser can reach the file last, and replay rebuilds a keyspace
that never existed — with the wrong value, silently, only under load, only
after a restart.

`TestJournalOrderMatchesMutationOrder` asserts the property directly: after 800
racing writes to one key, the last record in the log must name the value the
store ended up holding. Verified by mutation — moving the append outside the
lock makes it fail (on attempt 18 of 50, which is also a fair warning about how
hard this class of bug is to catch by luck).

The other half of the decision matters as much: **the fsync must not happen
under that gate.** An fsync can take tens of milliseconds; holding it for that
long would make every writer wait for one client's disk. So `Append` is a
memcpy and `Sync` is a separate call the server makes after the gate is
released.

### fsync policy: measured, not asserted

`redis-benchmark -t set -n 20000 -c 50` against this server, M1 Pro/APFS:

| `-appendfsync` | throughput | p50 latency | loses on `kill -9` |
|---|---|---|---|
| `always` | 777 rps | 53.7 ms | nothing |
| `everysec` | 61,920 rps | 0.30 ms | up to 1s of writes |
| `no` | 88,496 rps | 0.29 ms | up to ~30s (whatever the kernel holds) |
| *(AOF off)* | 89,286 rps | 0.29 ms | everything |

I also measured the loss directly: 20 writes then an immediate `kill -9` leaves
**0 bytes** on disk under `everysec` and **all 622 bytes** under `always`.

Reading these honestly:

- **`everysec` costs about 30%** against no durability at all, and is the
  default for the same reason it is Redis's: one second of exposure is a price
  almost everyone will pay, and the two-orders-of-magnitude cliff below is not.
- **`always` is 80× slower here, which overstates it.** Go's `File.Sync` on
  macOS issues `F_FULLFSYNC`, which forces the drive to flush its own write
  cache; Redis calls `fdatasync`, which on Linux typically returns once the data
  reaches the disk's cache. The comparison is honest about *this* build on *this*
  platform and would flatter `always` considerably on Linux — worth re-measuring
  in M5 rather than quoting 777 rps as a fact about the design.
- **`no` is nearly free** and is a genuinely different guarantee, not a weaker
  version of `everysec`: it survives the *process* dying (the kernel still has
  the data) and not the *machine* dying. The sync loop still runs under `no`, to
  push the userspace buffer to the kernel — otherwise a process crash would lose
  writes that the policy promises to keep.

### Torn tail: repair. Corruption: refuse.

**Chosen:** a log that ends mid-record is truncated to the last complete record
and the server starts. Damage anywhere earlier returns `ErrCorrupt` and the
server exits.

**Why the asymmetry:** a torn tail is the *expected* result of a crash during a
write — the machine lost power between `write` and the end of the record. The
lost command was, by definition, never acknowledged under any policy stronger
than `no`. Refusing to start would turn an ordinary power cut into an
unbootable server, which is a worse outcome than losing the write the client
never heard back about.

Corruption in the middle cannot be explained that way: it means a bad disk, a
truncated *middle*, or someone editing the file. Skipping it would rebuild a
keyspace that never existed and then append to it, compounding the damage.
Exiting is the only honest move; a human needs to look at the file. (Redis
draws the same line with `aof-load-truncated`.)

The repair is a real `os.Truncate`, not just "stop reading here": the next
append has to land on a record boundary, or the next restart sees corruption
instead of a tail.

### Finding the truncation point without a byte counter

Replay needs the offset of the last good record. The RESP reader buffers ahead,
so the file position is not the parser's position — the usual fix is to plumb a
counting reader through the parser and expose its offset.

**Chosen instead:** re-derive each applied command's encoded length
(`aof.EncodedLen`) and accumulate. That works only because this log is written
by our own encoder, so the encoding is canonical and the recomputation is exact
rather than an estimate. It keeps a durability concern out of the RESP package
entirely.

The risk is obvious — if `EncodedLen` and `Append` ever disagree, truncation
cuts in the wrong place — so `TestEncodedLenMatchesWhatIsWritten` writes 50
records and asserts the predicted total equals the file size.

### Replay runs the real command handlers

**Chosen:** replay feeds each record through the same dispatch table and
handlers that serve clients, with the reply written to a `bytes.Buffer` instead
of a socket.

**Rejected:** a separate interpreter for logged commands (a switch over SET /
DEL / PEXPIREAT).

**Why:** a second interpreter is a second implementation of every command's
semantics, free to drift from the first — and the failure mode of that drift is
a keyspace that is subtly wrong after every restart, which is close to the worst
bug this project could have. Reusing the handlers makes drift impossible by
construction.

What replay adds is validation the wire protocol does not need: the record must
be a **known** command, a **write** command (that is what the `writes` flag in
the dispatch table is for), and of the right arity — and the captured reply must
not start with `-`. That last check is the one I would have skipped and been
wrong to: without it, a record the server would refuse from a client (`SET k v
EX 0`) gets applied in silence at startup.

### A failed log refuses writes, rather than accepting them

Once a write to the AOF fails (a full disk, most likely), the error is latched
and every subsequent **write** command replies `-MISCONF Errors writing to the
AOF file`. Reads keep working.

The alternative — carry on serving and log a warning — means telling clients
`+OK` for data that will not survive a restart, which is the specific lie
durability exists to prevent. A degraded, read-only cache is a much better
failure mode than a lying one.

**The gap I can't close cheaply:** the *first* failing write has already
mutated memory by the time the error surfaces, so that one command gets an `+OK`
it does not deserve. Fixing it properly means either journalling before applying
(and then having to undo the record if the mutation fails) or a two-phase
commit. Redis has the same hole. It is worth naming rather than hiding.

### What is deliberately missing

- **No AOF rewrite (`BGREWRITEAOF`).** The log grows forever: a key written a
  million times has a million records. The fix is well understood — walk the
  keyspace, emit one `SET k v PXAT ...` per live key into a temp file, append
  anything that arrived during the walk, rename over the old log — but it needs
  a consistent snapshot to walk, which interacts with M4's eviction. Doing it
  now would mean doing it twice.
- **No RDB snapshots.** The AOF is the only persistence. RDB is faster to load
  and smaller on disk; it is also a second serialisation format for the same
  data, and M3 asked for durability, not for both.
- **The monotonic-clock retreat from M2 is now real.** Deadlines written to the
  log are absolute wall-clock milliseconds, so a replayed key's deadline no
  longer carries a monotonic reading — after a restart, deadlines are only as
  stable as the wall clock. In-process deadlines still are. This is inherent:
  no serialisation can carry another machine's (or another boot's) monotonic
  clock.

## M4 — maxmemory + LRU

### A keyspace estimate, not process RSS

**Chosen:** account for key bytes, value bytes, and a fixed 128-byte estimated
per-entry overhead; expose that estimate as `used_memory`.

**Rejected:** `runtime.ReadMemStats().HeapAlloc` as a hard budget.

**Why:** heap allocation includes connection buffers, Go runtime metadata, and
garbage waiting for a collection, none of which belongs to a key's eviction
decision. It is also global, so it cannot say which deletion freed which
bytes. Exact map and allocator accounting is unavailable in Go; the explicit
estimate is predictable, cheap, and tested against real heap growth to stay in
the right range. It changes on every overwrite and every deletion path, which
is more important than pretending it is exact.

### Sampled, coarse LRU

**Chosen:** sample five eligible keys and evict the one with the oldest atomic
logical access timestamp. The existing 100 ms expiry ticker advances the
clock.

**Rejected:** exact LRU with a linked list, and random eviction as the default.

**Why:** exact LRU moves a list node on every GET, turning the hot read path
into a write-locked path. The sampled approximation retains shared read locks
and beats random on the tested 80/20 locality workload. Five candidates is the
useful knee in the cost/quality curve; timestamps within a 100 ms bucket tie,
which is acceptable for a cache and avoids a time call per read.

### Policy and the one-write overage

**Chosen:** default to `noeviction`; enforce the prior command's overage before
the next write. `allkeys-lru`, `allkeys-random`, and `volatile-lru` are explicit
opt-ins.

**Rejected:** evicting by default, and predicting the next command's exact
allocation before applying it.

**Why:** silent loss is wrong for a database-shaped workload. A cache operator
can explicitly choose what is disposable, including TTL-only keys with
`volatile-lru`. Preflight enforcement keeps an OOM write unmodified while
allowing at most one command of overshoot; exact prediction would duplicate
all mutation logic and still miss Go allocator effects.

### Eviction is durable; expiry is not

**Chosen:** journal eviction as `DEL`; leave expiry collection unjournalled.

**Why:** an expiry is already implied by the absolute deadline in the AOF, so
replay reaches the same result. Eviction is a machine-local choice with no
such implication. Omitting its `DEL` would restore evicted keys on restart and
violate the configured bound immediately.

## M5 — Benchmarking & profiling

### Measure the wire server, then isolate the store

**Chosen:** compare with `redis-benchmark` on loopback using the same process,
client count, pipeline depth, and persistence settings; pair that with a
ten-second Go CPU profile of redisclone under the real network load.

**Rejected:** quoting the M1/M3 ad-hoc numbers as a general performance claim,
or profiling only a microbenchmark.

**Why:** a microbenchmark is excellent at assigning cost inside one component,
but it skips RESP parsing, connection goroutines, buffering, and syscalls.
The external workload says what an operator sees; pprof says where its time
goes. On the Apple M1 Pro, 50 clients and pipeline 16, redisclone measured
482,625 SET/s and 1,237,624 GET/s; Redis measured 1,162,791 and 1,424,501 on
the same workload. The CPU profile under sustained SET load identified the
single keyspace mutex as the write bottleneck: lock/unlock consumed about 36%
of cumulative sampled CPU. The store benchmark independently worsened from
128 ns/op to 375 ns/op for parallel SET from one to eight CPUs.

### Do not shard on a profile result alone

**Chosen:** leave the single `RWMutex` in place, and make it the next measured
design target rather than silently replacing it in M5.

**Rejected:** immediately striping the map into shards.

**Why:** the profile justifies investigating sharding, but not changing the
atomicity contract in the same breath. Multi-key DEL is currently atomic under
one lock; a sharded map needs a deterministic multi-lock protocol and new
tests to preserve that guarantee. M5's job was to produce evidence and an
honest bottleneck, not to hide a semantic redesign behind a throughput number.

### No speculative parser optimization

**Chosen:** leave RESP parsing and allocation behavior unchanged after the
stage-five sustained SET profile.

**Rejected:** a zero-copy parser, command-name fast paths, or buffer-pool work
without a measurable profile target.

**Why:** the profile put allocation at about 2.4% cumulative CPU and bulk
parsing below 2%; lock contention and network/syscall scheduling dominated.
Changing ownership rules or adding pools for that amount of CPU would make the
code harder to reason about while failing to move the measured bottleneck.

### Stage-six final: canonical shard ownership

**Chosen:** 32 FNV-1a-routed shards are the only keyspace state. A single-key
command locks its shard; `MSET` and `DEL` deduplicate shard IDs, lock in
ascending order, and unlock in reverse. A separate commit mutex serializes a
durable mutation and its AOF append when AOF is enabled.

**Rejected:** retaining mirrored global maps as a permanent coordination layer,
or appending after releasing a shard lock.

**Why:** mirrored maps turned every write into two writes and left the global
mutex on the hot path, defeating the purpose of sharding. Ordered shard locks
preserve whole-command `DEL`/`MSET` behavior without deadlock. The commit gate
keeps replay order equal to mutation order while letting no-AOF workloads run
unrelated shard writes concurrently. Appending outside that gate can replay a
losing same-key write last and rebuild a state the server never held.

**Expiry, eviction, and rewrite:** each shard owns its expiry index and memory
subtotal; the samplers walk those local structures. Evictions are logged as
`DEL`, while expiry is already represented by absolute deadlines. AOF rewrite
starts tail capture before its shard snapshot, so a racing mutation appears in
the snapshot, the tail, or both; duplicates are idempotent.

**Measured result:** on the M1 Pro store microbenchmark, SET improved from
184.7 ns/op at one CPU to 146.8 ns/op at eight CPUs. The matching wire
benchmark reached 1,060,445 SET/s and 1,290,323 GET/s. See `BENCHMARKS.md` for
the full command and latency figures.
