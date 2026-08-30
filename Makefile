# redisclone — Redis-compatible in-memory KV store in Go (stdlib only)

BIN     := bin/redisclone
PKG     := ./cmd/redisclone
PORT    ?= 6379
BENCH_ADDR ?= 127.0.0.1:6379
BENCH_N ?= 1000000
BENCH_C ?= 50
BENCH_P ?= 16
PPROF_ADDR ?= 127.0.0.1:6060
PROFILE_SECONDS ?= 10

.PHONY: build run test e2e fmt vet bench-store bench-wire profile-top clean

build:
	go build -o $(BIN) $(PKG)

run: build
	$(BIN)

# Unit tests (your own, next to your packages)
test:
	go test ./internal/... ./cmd/...

# Black-box end-to-end tests: builds the binary, boots it on a random port,
# speaks raw RESP bytes at it over TCP.
e2e:
	go test -v -count=1 ./test/e2e/...

fmt:
	gofmt -l -w .

vet:
	go vet ./...

# Store-only measurements; use this to identify changes in lock and allocation
# cost without network noise.
bench-store:
	go test ./internal/store -run '^$$' -bench 'Benchmark(Get|Set|Mixed)Parallel$$' -benchmem -cpu 1,8

# Requires redis-benchmark and a running redisclone instance at BENCH_ADDR.
# Example: make run & make bench-wire
bench-wire:
	redis-benchmark -h $(word 1,$(subst :, ,$(BENCH_ADDR))) -p $(word 2,$(subst :, ,$(BENCH_ADDR))) -t set,get -n $(BENCH_N) -c $(BENCH_C) -P $(BENCH_P) -q

# Start redisclone with -pprof-addr $(PPROF_ADDR), then collect a CPU summary.
profile-top:
	go tool pprof -top 'http://$(PPROF_ADDR)/debug/pprof/profile?seconds=$(PROFILE_SECONDS)'

clean:
	rm -rf bin
