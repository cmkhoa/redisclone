# Benchmarks

## M5 baseline — 2026-08-29

Machine: Apple M1 Pro, darwin/arm64, Go 1.26.1. Both servers used loopback,
50 clients, pipeline depth 16, and no persistence.

```sh
redis-benchmark -h 127.0.0.1 -p PORT -t set,get -n 1000000 -c 50 -P 16 -q
```

| Server | SET requests/s | GET requests/s |
| --- | ---: | ---: |
| redisclone | 482,625 | 1,237,624 |
| Redis | 1,162,791 | 1,424,501 |

For repeat runs, use `make bench-store` for store microbenchmarks and
`make bench-wire` for the wire workload. To profile a running redisclone,
start it with `-pprof-addr 127.0.0.1:6060` and run `make profile-top`.

## M6 shard checkpoint — 2026-08-29

Same machine and Go version as the M5 baseline. Store-only benchmark command:

```sh
make bench-store
```

| Benchmark | 1 CPU | 8 CPUs | allocs/op |
| --- | ---: | ---: | ---: |
| GET | 68.42 ns | 90.59 ns | 0 |
| SET | 184.7 ns | 146.8 ns | 3 |
| 9:1 mixed | 85.24 ns | 112.7 ns | 0 |

The important checkpoint result is parallel write scaling: SET improves from
184.7 ns at one CPU to 146.8 ns at eight, rather than worsening under a global
keyspace lock. These are microbenchmark results; repeat the wire benchmark
before making an end-to-end throughput claim.

## M6 wire checkpoint — 2026-08-30

Machine: Apple M1 Pro, darwin/arm64, Go 1.26.1. redisclone ran on loopback
with no persistence; `redis-benchmark` used 50 clients and pipeline depth 16.

```sh
redis-benchmark -h 127.0.0.1 -p 6390 -t set,get -n 1000000 -c 50 -P 16 -q
```

| Server | SET requests/s | SET p50 | GET requests/s | GET p50 |
| --- | ---: | ---: | ---: | ---: |
| redisclone | 1,060,445 | 0.423 ms | 1,290,323 | 0.327 ms |

The client warned that it could not fetch unsupported Redis `CONFIG` metadata;
the benchmark requests themselves completed normally. Against the M5 baseline
on the same machine, SET throughput is about 2.2× higher; GET is broadly
unchanged, as expected from a read-side shard migration that already landed.
