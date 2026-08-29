# redisclone — Redis-compatible in-memory KV store in Go (stdlib only)

BIN     := bin/redisclone
PKG     := ./cmd/redisclone
PORT    ?= 6379

.PHONY: build run test e2e fmt vet clean

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

clean:
	rm -rf bin
