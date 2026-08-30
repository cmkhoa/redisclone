package server

import (
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"redisclone/internal/store"
)

func TestConfigGet(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	s.Store().SetMemoryLimit(4096, store.PolicyAllKeysLRU)
	s.Configure(Config{AppendOnly: true, AppendFsync: "everysec", AppendFilename: "data.aof", Dir: "/data"})

	got := run(t, s, cmd("CONFIG", "GET", "maxmemory*"))
	want := "*4\r\n$9\r\nmaxmemory\r\n$4\r\n4096\r\n$16\r\nmaxmemory-policy\r\n$11\r\nallkeys-lru\r\n"
	if got != want {
		t.Errorf("CONFIG GET maxmemory* = %q, want %q", got, want)
	}
	if got := run(t, s, cmd("CONFIG", "SET", "maxmemory", "1")); !strings.HasPrefix(got, "-ERR unsupported") {
		t.Errorf("CONFIG SET = %q", got)
	}
}

func TestInfoReportsClientsAndCommands(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	run(t, s, cmd("PING")+cmd("GET", "missing"))
	got := run(t, s, cmd("INFO", "all"))
	for _, field := range []string{"# Clients", "connected_clients:0", "total_commands_processed:3"} {
		if !strings.Contains(got, field) {
			t.Errorf("INFO missing %q:\n%s", field, got)
		}
	}
}

func TestDrainClosesRemainingConnections(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.Serve(l) }()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	deadline := time.Now().Add(time.Second)
	for s.ConnectedClients() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.ConnectedClients() != 1 {
		t.Fatal("server did not register client")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
	s.Drain(0)
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Error("connection remained open after drain")
	}
}
