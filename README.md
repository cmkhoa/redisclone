# redisclone

A small Redis-compatible in-memory key-value server written in Go using only
the standard library. It is a systems-programming project focused on clear,
defensible design choices.

## Features

- RESP2 over TCP; works with `redis-cli`
- `PING`, `ECHO`, `GET`, `SET`, `DEL`, `EXISTS`
- Key expiry: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`
- Append-only-file durability with `always`, `everysec`, and `no` fsync modes
- Memory limits with `noeviction`, `allkeys-lru`, `allkeys-random`, and
  `volatile-lru` policies
- `INFO` and `DBSIZE`

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
```

See [PLANNING.md](PLANNING.md) for milestone details and
[DECISIONS.md](DECISIONS.md) for the design rationale.
