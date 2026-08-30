# redisclone

A small Redis-compatible in-memory key-value server written in Go using only
the standard library. It is a systems-programming project focused on clear,
defensible design choices.

## Features

- RESP2 over TCP; works with `redis-cli`
- `PING`, `ECHO`, `GET`, `SET`, `MGET`, `MSET`, `DEL`, `EXISTS`
- String operations: `INCR`, `DECR`, `APPEND`, `STRLEN`
- Key expiry: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`
- Append-only-file durability with `always`, `everysec`, and `no` fsync modes
- `BGREWRITEAOF` compacts the append-only file without stopping writes
- Memory limits with `noeviction`, `allkeys-lru`, `allkeys-random`, and
  `volatile-lru` policies
- 32-shard canonical keyspace with ordered multi-key locking and concurrent
  single-key operations
- `INFO` and `DBSIZE`
- `CONFIG GET` for supported startup settings; graceful SIGINT/SIGTERM shutdown

## Architecture

Each client connection is handled by a goroutine. Commands are parsed and
written as RESP2 through buffered I/O, while the store provides the concurrency
and persistence boundaries.

```text
TCP client
  → RESP reader
  → command dispatch
  → sharded store (32 FNV-1a-routed shards)
  → RESP writer

mutations → AOF commit gate → buffered append-only file → background fsync
```

- A shard owns its key/value map, expiry index, approximate memory subtotal,
  and RWMutex. Single-key commands touch one shard.
- `DEL` and `MSET` deduplicate their shard IDs, lock in ascending order, and
  unlock in reverse, preserving whole-command behavior without deadlock.
- Expiry is lazy on reads plus active sampling of each shard's volatile-key
  index. Eviction samples shard-local candidates; evictions are written to the
  AOF as `DEL`.
- With AOF enabled, a small commit gate orders a mutation and its log record.
  `BGREWRITEAOF` snapshots shards while retaining concurrent writes in a tail,
  then atomically installs the replacement file.

## Recent changes

Stage 6 replaced the original global keyspace map and mutex with the canonical
32-shard store. Reads, writes, expiry, eviction, accounting, AOF snapshots,
and rewrite behavior now operate on shard-owned state. The migration also added
deterministic multi-key locking, shard parity/race coverage, and refreshed
store and wire benchmarks. See [BENCHMARKS.md](BENCHMARKS.md) and
[DECISIONS.md](DECISIONS.md) for the measured results and trade-offs.

## Run

```sh
make run
redis-cli -p 6379 PING
```

Useful options:

```sh
bin/redisclone \
  -appendonly -appendfsync everysec \
  -maxmemory 64mb -maxmemory-policy allkeys-lru
```

Use `-pprof-addr 127.0.0.1:6060` to expose Go's standard pprof endpoints for
local profiling.

## Verify

```sh
make test
make e2e
go test -race ./internal/... ./cmd/...
make bench-store
```

With a running server and `redis-benchmark` installed, run the M5 wire
workload with `make bench-wire` (override `BENCH_ADDR`, `BENCH_N`, `BENCH_C`,
or `BENCH_P` as needed). Start with `-pprof-addr 127.0.0.1:6060`, then run
`make profile-top` for a CPU summary.

See [PLANNING.md](PLANNING.md) for milestone details and
[DECISIONS.md](DECISIONS.md) for the design rationale.

## TODO

- Repeat the wire benchmark against Redis on the exact M6 workload before
  publishing a like-for-like comparison.
- Add authentication, TLS, connection/request/output-buffer limits, metrics
  export, and structured logging if the server leaves a trusted local network.
- Broaden Redis compatibility only when a concrete use case justifies the
  added command surface; this project intentionally remains a focused subset.
