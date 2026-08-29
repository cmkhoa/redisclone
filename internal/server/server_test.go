package server

import (
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

// These tests drive HandleConn over net.Pipe — a real net.Conn with no kernel
// sockets involved. The e2e suite covers the same commands over TCP; this
// covers the dispatch and error-policy decisions the e2e suite deliberately
// cannot see, and runs in microseconds.

// exchange runs one connection: it writes req, then reads until the server
// closes or goes quiet, and returns everything it said.
func exchange(t *testing.T, req string) string {
	t.Helper()

	client, server := net.Pipe()
	go New(log.New(io.Discard, "", 0)).HandleConn(server)

	go func() {
		client.Write([]byte(req))
		// Half-close is not available on a pipe, so signal "no more commands"
		// by closing once the server has had time to reply.
		time.Sleep(100 * time.Millisecond)
		client.Close()
	}()

	client.SetDeadline(time.Now().Add(2 * time.Second))
	got, err := io.ReadAll(client)
	if err != nil && !strings.Contains(err.Error(), "closed") && err != io.EOF {
		t.Fatalf("read reply: %v", err)
	}
	return string(got)
}

func TestDispatch(t *testing.T) {
	tests := []struct {
		name string
		req  string
		want string
	}{
		{"ping", "*1\r\n$4\r\nPING\r\n", "+PONG\r\n"},
		{"ping lowercase", "*1\r\n$4\r\nping\r\n", "+PONG\r\n"},
		{"ping mixed case", "*1\r\n$4\r\nPiNg\r\n", "+PONG\r\n"},
		{"ping with message", "*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n", "$2\r\nhi\r\n"},
		{"echo", "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", "$5\r\nhello\r\n"},
		{"echo empty", "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n", "$0\r\n\r\n"},
		// Pipelined commands reply in order, and "*0\r\n" between them is
		// skipped without a reply.
		{"pipelined", "*1\r\n$4\r\nPING\r\n*0\r\n*2\r\n$4\r\nECHO\r\n$1\r\na\r\n", "+PONG\r\n$1\r\na\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exchange(t, tt.req); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// A command error is a reply, not a disconnect: the -ERR is followed by the
// reply to the next command on the same connection.
func TestCommandErrorsKeepTheConnection(t *testing.T) {
	tests := []struct {
		name string
		req  string
		want string
	}{
		{"unknown command", "*2\r\n$12\r\nFLYTOTHEMOON\r\n$3\r\nnow\r\n",
			"-ERR unknown command 'FLYTOTHEMOON'\r\n+PONG\r\n"},
		{"echo without argument", "*1\r\n$4\r\nECHO\r\n",
			"-ERR wrong number of arguments for 'echo' command\r\n+PONG\r\n"},
		{"echo with two arguments", "*3\r\n$4\r\nECHO\r\n$1\r\na\r\n$1\r\nb\r\n",
			"-ERR wrong number of arguments for 'echo' command\r\n+PONG\r\n"},
		{"ping with two arguments", "*3\r\n$4\r\nPING\r\n$1\r\na\r\n$1\r\nb\r\n",
			"-ERR wrong number of arguments for 'ping' command\r\n+PONG\r\n"},
		// What a modern redis-cli opens with; -ERR makes it fall back to RESP2.
		{"hello 3", "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n",
			"-ERR unknown command 'HELLO'\r\n+PONG\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exchange(t, tt.req+"*1\r\n$4\r\nPING\r\n")
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// A protocol error is the other half of the policy: reply, then hang up,
// because the stream position is no longer known. The trailing PING must go
// unanswered.
func TestProtocolErrorClosesTheConnection(t *testing.T) {
	for _, req := range []string{
		"this is not resp\r\n",
		"*1\r\n$-5\r\n",
		"*-1\r\n",
	} {
		got := exchange(t, req+"*1\r\n$4\r\nPING\r\n")
		if !strings.HasPrefix(got, "-ERR Protocol error: ") {
			t.Errorf("%q: got %q, want an -ERR Protocol error reply", req, got)
		}
		if strings.Contains(got, "PONG") {
			t.Errorf("%q: server kept serving after a protocol error (%q)", req, got)
		}
	}
}

// A command name from the wire ends up inside a CRLF-terminated error line, so
// a client must not be able to smuggle a fake reply into the stream through it.
func TestUnknownCommandNameCannotInjectAReply(t *testing.T) {
	got := exchange(t, "*1\r\n$10\r\nBAD\r\n+PONG\r\n")
	if strings.Count(got, "\r\n") != 1 {
		t.Errorf("reply is not a single line: %q", got)
	}
	if got != "-ERR unknown command 'BAD..+PONG'\r\n" {
		t.Errorf("got %q", got)
	}
}

// A clean disconnect between commands is not an error and must not log noise.
func TestCleanDisconnect(t *testing.T) {
	client, server := net.Pipe()
	var logged strings.Builder
	done := make(chan struct{})
	go func() {
		New(log.New(&logged, "", 0)).HandleConn(server)
		close(done)
	}()

	client.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	buf := make([]byte, len("+PONG\r\n"))
	client.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	client.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not return after the client disconnected")
	}
	if logged.Len() != 0 {
		t.Errorf("clean disconnect logged: %s", logged.String())
	}
}

func TestSafeName(t *testing.T) {
	tests := []struct{ in, want string }{
		{"GET", "GET"},
		{"a\r\nb", "a..b"},
		{"a\x00b", "a.b"},
		{"quote'd", "quote.d"},
		{strings.Repeat("x", 200), strings.Repeat("x", 128) + "..."},
	}
	for _, tt := range tests {
		if got := safeName(tt.in); got != tt.want {
			t.Errorf("safeName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
