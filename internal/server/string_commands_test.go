package server

import (
	"io"
	"log"
	"strings"
	"testing"
	"time"
)

func TestBatchAndStringCommands(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	req := cmd("MSET", "a", "1", "b", "two") +
		cmd("MGET", "a", "missing", "b") +
		cmd("INCR", "a") + cmd("DECR", "a") +
		cmd("APPEND", "b", "!") + cmd("STRLEN", "b")
	want := "+OK\r\n*3\r\n$1\r\n1\r\n$-1\r\n$3\r\ntwo\r\n" +
		":2\r\n:1\r\n:4\r\n:4\r\n"
	if got := run(t, s, req); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestStringCommandsErrorsAndExpiry(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	if got := run(t, s, cmd("SET", "word", "not-a-number")+cmd("INCR", "word")); got != "+OK\r\n-ERR value is not an integer or out of range\r\n" {
		t.Errorf("INCR error = %q", got)
	}
	if got := run(t, s, cmd("SET", "max", "9223372036854775807")+cmd("INCR", "max")); got != "+OK\r\n-ERR value is not an integer or out of range\r\n" {
		t.Errorf("INCR overflow = %q", got)
	}

	if got := run(t, s, cmd("SET", "ttl", "a", "PX", "1000")+cmd("APPEND", "ttl", "b")+cmd("PTTL", "ttl")); !strings.HasPrefix(got, "+OK\r\n:2\r\n:") {
		t.Errorf("APPEND did not preserve TTL: %q", got)
	}
	// The returned value predates APPEND and must not be changed in place.
	s.Store().Set("held", []byte("a"))
	held, _ := s.Store().Get("held")
	run(t, s, cmd("APPEND", "held", "b"))
	if string(held) != "a" {
		t.Errorf("held value mutated to %q", held)
	}

	// Let a short-lived value expire before testing the missing-key length.
	run(t, s, cmd("SET", "gone", "v", "PX", "1"))
	time.Sleep(2 * time.Millisecond)
	if got := run(t, s, cmd("STRLEN", "gone")); got != ":0\r\n" {
		t.Errorf("STRLEN expired = %q", got)
	}
}

func TestMSetRequiresPairs(t *testing.T) {
	s := New(log.New(io.Discard, "", 0))
	if got := run(t, s, cmd("MSET", "only-key")); got != "-ERR wrong number of arguments for 'mset' command\r\n" {
		t.Errorf("MSET odd arguments = %q", got)
	}
}
