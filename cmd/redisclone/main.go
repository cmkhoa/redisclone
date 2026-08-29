// Command redisclone is a Redis-compatible in-memory key-value server.
//
// RESP2 wire protocol; PING/ECHO, GET/SET/DEL/EXISTS, and key expiration.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	_ "net/http/pprof"

	"redisclone/internal/aof"
	"redisclone/internal/server"
	"redisclone/internal/store"
)

func main() {
	addr := flag.String("addr", ":6379", "address to listen on (host:port)")
	appendOnly := flag.Bool("appendonly", false, "log every write to an append-only file and replay it at startup")
	appendFsync := flag.String("appendfsync", "everysec", "when to fsync the append-only file: always, everysec or no")
	appendFilename := flag.String("appendfilename", "appendonly.aof", "name of the append-only file")
	dir := flag.String("dir", ".", "directory the append-only file lives in")
	maxMemory := flag.String("maxmemory", "0", "maximum memory for the keyspace (for example 64mb; 0 is unlimited)")
	maxMemoryPolicy := flag.String("maxmemory-policy", "noeviction", "eviction policy: noeviction, allkeys-lru, allkeys-random, volatile-lru")
	pprofAddr := flag.String("pprof-addr", "", "optional address for Go pprof HTTP endpoints (for example 127.0.0.1:6060)")
	flag.Parse()

	logger := log.New(os.Stderr, "", log.LstdFlags)
	if *pprofAddr != "" {
		go func() {
			logger.Printf("pprof listening on %s", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				logger.Printf("pprof server: %v", err)
			}
		}()
	}

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Fatalf("listen %s: %v", *addr, err)
	}
	logger.Printf("redisclone listening on %s", l.Addr())

	srv := server.New(logger)
	max, err := store.ParseMemory(*maxMemory)
	if err != nil {
		logger.Fatalf("-maxmemory: %v", err)
	}
	evictionPolicy, err := store.ParseEvictionPolicy(*maxMemoryPolicy)
	if err != nil {
		logger.Fatalf("-maxmemory-policy: %v", err)
	}
	srv.Store().SetMemoryLimit(max, evictionPolicy)

	if *appendOnly {
		policy, err := aof.ParsePolicy(*appendFsync)
		if err != nil {
			logger.Fatalf("-appendfsync: %v", err)
		}
		path := filepath.Join(*dir, *appendFilename)

		// Replay first, attach second: the store must not journal what it is
		// in the middle of reading back.
		start := time.Now()
		res, err := srv.ReplayAOF(path)
		if err != nil {
			// A corrupt log is fatal. Starting anyway would serve a keyspace
			// that silently disagrees with the log, and the next write would
			// append to a file we have already decided we cannot read.
			logger.Fatalf("replaying %s: %v", path, err)
		}
		if res.Truncated > 0 {
			logger.Printf("aof: discarded %d bytes of torn tail (a crash mid-write); %d commands replayed",
				res.Truncated, res.Commands)
		}
		if res.Commands > 0 {
			logger.Printf("aof: replayed %d commands in %s", res.Commands, time.Since(start).Round(time.Millisecond))
		}

		l, err := aof.Open(path, policy)
		if err != nil {
			logger.Fatalf("opening %s: %v", path, err)
		}
		defer l.Close()
		srv.AttachAOF(l)
		logger.Printf("aof: %s, fsync %s", path, policy)
	}

	// Housekeeping goroutines (today: active key expiration) live as long as
	// this context does.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv.StartBackgroundTasks(ctx)

	// Closing the listener makes Serve's Accept fail with net.ErrClosed, which
	// it treats as a clean shutdown. In-flight connections are not drained yet
	// — there is no state to lose until M3's AOF.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-stop
		logger.Printf("received %s, shutting down", sig)
		cancel()
		l.Close()
	}()

	if err := srv.Serve(l); err != nil {
		logger.Fatalf("serve: %v", err)
	}
	// Serve returned because the listener closed, i.e. we are shutting down.
	// The deferred Close on the log flushes and fsyncs what is still buffered.
}
