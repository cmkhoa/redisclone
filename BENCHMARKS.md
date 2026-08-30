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
