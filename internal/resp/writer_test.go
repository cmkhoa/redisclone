package resp

import (
	"bytes"
	"io"
	"math"
	"testing"
)

// The pattern: call one Write method, compare the bytes it produced against a
// hand-written literal from the spec. No RESP parsing in the test — if the
// test needed a decoder to check the encoder, a matching pair of bugs would
// cancel out and pass.

func TestWriteSimpleString(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"ok", "OK", "+OK\r\n"},
		{"pong", "PONG", "+PONG\r\n"},
		{"empty", "", "+\r\n"},
		{"spaces", "hello world", "+hello world\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := NewWriter(&buf).WriteSimpleString(tt.in); err != nil {
				t.Fatalf("WriteSimpleString(%q) returned error: %v", tt.in, err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("WriteSimpleString(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestWriteSimpleStringRejectsNewline(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("WriteSimpleString with an embedded CRLF did not panic")
		}
	}()
	_ = NewWriter(&bytes.Buffer{}).WriteSimpleString("bad\r\nvalue")
}

// Short writes and mid-write failures have to surface, not get swallowed: a
// client that disconnects mid-reply shows up here as a write error.
func TestWriteSimpleStringPropagatesError(t *testing.T) {
	if err := NewWriter(failingWriter{}).WriteSimpleString("OK"); err == nil {
		t.Error("expected the underlying write error, got nil")
	}
}

type failingWriter struct{}

func (failingWriter) Write(p []byte) (int, error) {
	return 0, errBrokenPipe
}

var errBrokenPipe = &writeError{"broken pipe"}

type writeError struct{ msg string }

func (e *writeError) Error() string { return e.msg }

// ---------------------------------------------------------------------------
// The remaining four types. Same pattern: one call, compare against the exact
// byte string the spec requires.
// ---------------------------------------------------------------------------

func TestWriteError(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter(&buf).WriteError("ERR unknown command 'FOO'"); err != nil {
		t.Fatalf("WriteError returned error: %v", err)
	}
	if got, want := buf.String(), "-ERR unknown command 'FOO'\r\n"; got != want {
		t.Errorf("WriteError = %q, want %q", got, want)
	}
}

func TestWriteErrorRejectsNewline(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("WriteError with an embedded CRLF did not panic")
		}
	}()
	_ = NewWriter(&bytes.Buffer{}).WriteError("ERR bad\r\n+INJECTED")
}

func TestWriteInteger(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{0, ":0\r\n"},
		{1, ":1\r\n"},
		{1000, ":1000\r\n"},
		{-1, ":-1\r\n"},
		{math.MaxInt64, ":9223372036854775807\r\n"},
		{math.MinInt64, ":-9223372036854775808\r\n"}, // 20 bytes: the scratch-buffer worst case
	}

	for _, tt := range tests {
		var buf bytes.Buffer
		if err := NewWriter(&buf).WriteInteger(tt.in); err != nil {
			t.Fatalf("WriteInteger(%d) returned error: %v", tt.in, err)
		}
		if got := buf.String(); got != tt.want {
			t.Errorf("WriteInteger(%d) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestWriteBulkString(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want string
	}{
		{"hello", []byte("hello"), "$5\r\nhello\r\n"},
		{"empty", []byte(""), "$0\r\n\r\n"},
		{"nil is empty, not null", nil, "$0\r\n\r\n"},
		{"embedded crlf", []byte("a\r\nb"), "$4\r\na\r\nb\r\n"},
		{"nul byte", []byte("a\x00b"), "$3\r\na\x00b\r\n"},
		// Length is a byte count, not a rune count: "héllo" is 6 bytes.
		{"multibyte", []byte("héllo"), "$6\r\nhéllo\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := NewWriter(&buf).WriteBulkString(tt.in); err != nil {
				t.Fatalf("WriteBulkString(%q) returned error: %v", tt.in, err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("WriteBulkString(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestWriteNulls(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteNullBulkString(); err != nil {
		t.Fatalf("WriteNullBulkString: %v", err)
	}
	if err := w.WriteNullArray(); err != nil {
		t.Fatalf("WriteNullArray: %v", err)
	}
	if got, want := buf.String(), "$-1\r\n*-1\r\n"; got != want {
		t.Errorf("nulls = %q, want %q", got, want)
	}
}

func TestWriteArrayHeader(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteArrayHeader(2); err != nil {
		t.Fatalf("WriteArrayHeader: %v", err)
	}
	for _, s := range []string{"ECHO", "hi"} {
		if err := w.WriteBulkString([]byte(s)); err != nil {
			t.Fatalf("WriteBulkString: %v", err)
		}
	}
	if got, want := buf.String(), "*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"; got != want {
		t.Errorf("array = %q, want %q", got, want)
	}

	buf.Reset()
	if err := NewWriter(&buf).WriteArrayHeader(0); err != nil {
		t.Fatalf("WriteArrayHeader(0): %v", err)
	}
	if got, want := buf.String(), "*0\r\n"; got != want {
		t.Errorf("empty array = %q, want %q", got, want)
	}
}

// A mixed nested reply, compared as one byte string: the shape M1's real
// replies take, and the check that headers and elements interleave correctly.
func TestWriteNestedReply(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteArrayHeader(3); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteInteger(42); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulkString([]byte("val")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteNullBulkString(); err != nil {
		t.Fatal(err)
	}

	want := "*3\r\n:42\r\n$3\r\nval\r\n$-1\r\n"
	if got := buf.String(); got != want {
		t.Errorf("nested reply = %q, want %q", got, want)
	}
}

// The encoder is on the hot path of every reply, so it must not allocate.
func TestWriteAllocations(t *testing.T) {
	w := NewWriter(io.Discard)
	payload := []byte("hello")
	allocs := testing.AllocsPerRun(100, func() {
		_ = w.WriteInteger(1000)
		_ = w.WriteBulkString(payload)
		_ = w.WriteArrayHeader(2)
	})
	if allocs != 0 {
		t.Errorf("encoding allocated %.1f times per run, want 0", allocs)
	}
}
