# Improvement Roadmap

Work from low-risk, self-contained changes toward changes that alter the
keyspace architecture. Each stage should finish with `make test`, `make e2e`,
and `go test -race ./internal/... ./cmd/...` green.

## 1. Make measurement repeatable — complete

**Scope:** tooling only; no server behavior changes.

- Add Make targets or a small script for the M5 `redis-benchmark` workload.
- Save the command, machine details, request rate, and latency in a dated
  benchmark note.
- Add a pprof capture command using `-pprof-addr`.

**Done when:** a developer can repeat the Redis comparison and CPU profile
without reconstructing shell commands from project history.

## 2. Add small, high-value commands — complete

**Scope:** extend the command table and store API without changing storage
layout.

- `MGET` and `MSET` for common batched client work.
- `INCR`/`DECR` with Redis-compatible integer parsing and overflow errors.
- `APPEND` and `STRLEN`.

`INCR` and `APPEND` must use copy-on-write: callers may still hold a slice
returned by `GET`, so stored bytes must never be modified in place.

**Done when:** unit and black-box tests cover normal, binary-safe, wrong-type,
overflow, duplicate-key, and concurrent cases.

## 3. Improve operational control

**Scope:** observability and safe runtime configuration.

- Extend `INFO` with command, connection, and persistence metrics.
- Add `CONFIG GET` for the supported startup settings.
- Add graceful shutdown that stops accepting clients, flushes the AOF, and
  waits briefly for active requests.

**Done when:** an operator can inspect the configured limits and durability
state through Redis commands, and a normal termination does not lose buffered
AOF data.

## 4. Compact the AOF

**Scope:** durability internals; preserve existing client semantics.

- Implement `BGREWRITEAOF`.
- Take a consistent keyspace snapshot and write one `SET ... PXAT` record per
  live key into a temporary AOF.
- Preserve mutations that arrive during the rewrite, then atomically replace
  the old file.
- Exclude expired keys and ensure eviction `DEL`s are not lost.

**Done when:** e2e tests prove that a rewrite shrinks a churned log, survives
restart, preserves TTLs, and remains correct while clients write concurrently.

## 5. Reduce easy hot-path overhead

**Scope:** performance work that does not alter locking semantics.

- Use M5 profiles to target allocation or parsing costs only when they appear
  in the profile.
- Benchmark any proposed change against the existing M5 workload and store
  microbenchmarks.
- Keep an optimization only if it improves the relevant workload without
  regressing correctness or readability disproportionately.

**Done when:** every accepted optimization has before/after numbers recorded
in `DECISIONS.md`.

## 6. Shard the keyspace

**Scope:** major architecture change; do this only after the earlier behavior
is well covered.

- Split the keyspace into 32–64 shards, each with its own lock, data map,
  expiry index, LRU metadata, and memory subtotal.
- Route single-key commands to one shard.
- For multi-key operations, sort shard IDs before acquiring locks. This
  prevents deadlock and preserves the current atomic `DEL` behavior.
- Rework active expiration, global memory accounting, and sampled eviction so
  they select candidates across shards.
- Retain AOF mutation ordering despite concurrent shard writes.

**Done when:** race and e2e coverage proves the old semantics, M5 shows a
meaningful write-throughput gain, and the new complexity is documented in
`DECISIONS.md`.

## 7. Production-facing features

**Scope:** only if the server will leave a trusted local network.

- Authentication and TLS.
- Resource limits for connections, request size, and client output buffers.
- Metrics export, structured logging, and deployment configuration.

**Done when:** the threat model and operational requirements are defined;
these features should not be added speculatively to a learning project.
