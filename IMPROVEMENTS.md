# Improvements

## Highest priority: shard the keyspace

M5 identified the single store mutex as the write-path bottleneck under
concurrent load. The next performance project is a sharded keyspace:

1. Use 32–64 shards, each with its own lock, data map, and expiry index.
2. Route single-key commands to one shard.
3. Sort shard IDs and lock in that order for multi-key commands so `DEL`
   remains atomic and deadlock-free.
4. Adapt memory accounting and eviction to select candidates across shards.
5. Re-run race, e2e, and the M5 benchmark before and after.

## Other worthwhile work

- Add `BGREWRITEAOF` to compact the append-only file.
- Add common Redis commands such as `MGET`, `MSET`, `INCR`, and `APPEND`.
- Add snapshot persistence if faster startup becomes important.
- Add authentication and TLS before exposing the server beyond a trusted
  network.
- Automate the Redis comparison benchmark in CI or a repeatable local script.
