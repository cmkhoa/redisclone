package resp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// No sockets in here: a RESP reader consumes an io.Reader, so strings.NewReader
// is a complete stand-in for a client and the tests stay instant.

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"ping", "*1\r\n$4\r\nPING\r\n", []string{"PING"}},
		{"echo", "*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n", []string{"ECHO", "hi"}},
		{"empty argument", "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n", []string{"ECHO", ""}},
		{"lowercase preserved", "*1\r\n$4\r\nping\r\n", []string{"ping"}},
		// The payload is length-prefixed, so CR, LF and NUL are just bytes.
		{"binary safe", "*2\r\n$4\r\nECHO\r\n$8\r\na\r\nb\x00c\rd\r\n", []string{"ECHO", "a\r\nb\x00c\rd"}},
		{"many args", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", []string{"SET", "k", "v"}},
		{"empty array", "*0\r\n", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tt.in)).ReadCommand()
			if err != nil {
				t.Fatalf("ReadCommand(%q): %v", tt.in, err)
			}
			if !equal(got, tt.want) {
				t.Errorf("ReadCommand(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// Pipelining: several commands in one buffer must come out one at a time, in
// order, with the reader keeping its place in the stream.
func TestReadCommandPipelined(t *testing.T) {
	in := "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$1\r\na\r\n*1\r\n$4\r\nPING\r\n"
	r := NewReader(strings.NewReader(in))

	for i, want := range [][]string{{"PING"}, {"ECHO", "a"}, {"PING"}} {
		got, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if !equal(got, want) {
			t.Errorf("command %d = %q, want %q", i, got, want)
		}
	}
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Errorf("after the last command: got %v, want io.EOF", err)
	}
}

// A clean disconnect at a command boundary is io.EOF and nothing else — that
// is what tells the server this was a normal goodbye.
func TestReadCommandEOFAtBoundary(t *testing.T) {
	if _, err := NewReader(strings.NewReader("")).ReadCommand(); !errors.Is(err, io.EOF) {
		t.Errorf("got %v, want io.EOF", err)
	}
}

// Truncation mid-command is distinguishable from a clean goodbye.
func TestReadCommandTruncated(t *testing.T) {
	truncations := []string{
		"*",
		"*2\r\n",
		"*2\r\n$4\r\n",
		"*2\r\n$4\r\nECHO\r\n",
		"*2\r\n$4\r\nECHO\r\n$5\r\nhel",
		"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r",
		"*1\r\n$4\r\nPING",
	}
	for _, in := range truncations {
		_, err := NewReader(strings.NewReader(in)).ReadCommand()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("ReadCommand(%q) = %v, want io.ErrUnexpectedEOF", in, err)
		}
	}
}

func TestReadCommandProtocolErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"not an array", "+OK\r\n"},
		{"inline command", "PING\r\n"},
		{"garbage", "this is not resp\r\n"},
		{"bare LF", "*1\n$4\r\nPING\r\n"},
		{"empty line", "\r\n"},
		{"null array", "*-1\r\n"},
		{"negative array count", "*-5\r\n"},
		{"non-numeric array count", "*abc\r\n"},
		{"array count with spaces", "* 1\r\n"},
		{"element is not a bulk string", "*1\r\n+PING\r\n"},
		{"negative bulk length", "*1\r\n$-5\r\n"},
		{"null bulk as argument", "*1\r\n$-1\r\n"},
		{"non-numeric bulk length", "*1\r\n$abc\r\n"},
		{"bulk length too large", fmt.Sprintf("*1\r\n$%d\r\n", MaxBulkLength+1)},
		{"array count too large", fmt.Sprintf("*%d\r\n", MaxArrayLength+1)},
		{"overflowing bulk length", "*1\r\n$99999999999999999999\r\n"},
		{"payload not CRLF-terminated", "*1\r\n$4\r\nPINGxx"},
		{"payload terminated by bare LF", "*1\r\n$4\r\nPING\n\n"},
		{"header line never ends", "*1\r\n$" + strings.Repeat("9", readerBufSize)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tt.in)).ReadCommand()
			if err == nil {
				t.Fatalf("ReadCommand(%q) succeeded, want a protocol error", tt.in)
			}
			if !IsProtocolError(err) {
				t.Fatalf("ReadCommand(%q) = %v (%T), want a *ProtocolError", tt.in, err, err)
			}
			// The message goes onto the wire inside a "-ERR ...\r\n" line.
			if msg := err.Error(); strings.ContainsAny(msg, "\r\n") {
				t.Errorf("protocol error message contains CR or LF: %q", msg)
			}
		})
	}
}

// An oversized length must be rejected from the header alone, before anything
// is allocated: "$536870913" is otherwise a one-line memory exhaustion attack.
func TestReadCommandRejectsHugeLengthWithoutAllocating(t *testing.T) {
	in := fmt.Sprintf("*1\r\n$%d\r\n", MaxBulkLength+1)
	allocs := testing.AllocsPerRun(10, func() {
		r := NewReader(strings.NewReader(in))
		if _, err := r.ReadCommand(); err == nil {
			t.Fatal("expected a protocol error")
		}
	})
	// The reader's own 16 KiB buffer and the error value are allocated; a
	// 512 MiB payload buffer would be a different order of magnitude.
	if allocs > 20 {
		t.Errorf("rejecting an oversized header allocated %.0f times, want a handful", allocs)
	}
}

// Arguments must not alias the read buffer: M1's store keeps them.
func TestReadCommandArgsDoNotAliasBuffer(t *testing.T) {
	in := "*2\r\n$3\r\nSET\r\n$5\r\nfirst\r\n" + "*2\r\n$3\r\nSET\r\n$6\r\nsecond\r\n"
	r := NewReader(strings.NewReader(in))

	first, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	kept := first[1]
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	if string(kept) != "first" {
		t.Errorf("argument held across a read became %q, want %q", kept, "first")
	}
}

// TCP gives no message boundaries, so the reader must cope with a stream that
// hands back one byte at a time.
func TestReadCommandFragmented(t *testing.T) {
	in := "*2\r\n$4\r\nECHO\r\n$4\r\nfrag\r\n"
	got, err := NewReader(oneByteAtATime{strings.NewReader(in)}).ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if !equal(got, []string{"ECHO", "frag"}) {
		t.Errorf("got %q, want [ECHO frag]", got)
	}
}

type oneByteAtATime struct{ r io.Reader }

func (o oneByteAtATime) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}

// A payload larger than the read buffer exercises the io.ReadFull path rather
// than the buffered fast path.
func TestReadCommandLargePayload(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB
	in := fmt.Sprintf("*2\r\n$4\r\nECHO\r\n$%d\r\n%s\r\n", len(payload), payload)

	got, err := NewReader(strings.NewReader(in)).ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(got) != 2 || !bytes.Equal(got[1], payload) {
		t.Errorf("1 MiB payload did not round-trip (got %d args, arg1 len %d)", len(got), len(got[1]))
	}
}

func equal(got [][]byte, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if string(got[i]) != want[i] {
			return false
		}
	}
	return true
}
