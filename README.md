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
