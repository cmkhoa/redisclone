// Package resp implements RESP2 encoding and decoding.
//
// Spec: https://redis.io/docs/latest/develop/reference/protocol-spec/
package resp

import (
	"io"
	"strconv"
)

// crlf terminates every RESP value. Both bytes, always.
var crlf = []byte("\r\n")

// Writer encodes RESP values onto an underlying io.Writer.
//
// It does no buffering of its own: wrap a *bufio.Writer if you want batching,
// and Flush it yourself once the whole reply is written.
//
// Not safe for concurrent use — one Writer per connection.
type Writer struct {
	w io.Writer
	// scratch formats length and count lines. It lives here rather than on
	// writeCountLine's stack because passing a local array to w.Write (an
	// interface call) forces it to escape, costing an allocation per reply.
	scratch [24]byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// WriteSimpleString writes s as a RESP simple string: "+OK\r\n".
//
// Simple strings cannot contain CR or LF — there is no length prefix, so the
// first CRLF terminates the value. Callers control these strings (they are
// status replies like OK and PONG), so a caller passing a newline is a bug in
// this program, not bad input from the network. Hence the panic rather than an
// error return; user-supplied data belongs in a bulk string.
func (w *Writer) WriteSimpleString(s string) error {
	for i := 0; i < len(s); i++ {
		if s[i] == '\r' || s[i] == '\n' {
			panic("resp: CR or LF in simple string; use a bulk string")
		}
	}
	if _, err := io.WriteString(w.w, "+"); err != nil {
		return err
	}
	if _, err := io.WriteString(w.w, s); err != nil {
		return err
	}
	_, err := w.w.Write(crlf)
	return err
}

// WriteError writes msg as a RESP error: "-ERR unknown command 'FOO'\r\n".
//
// Same wire shape as a simple string, so the same CR/LF restriction applies —
// and the same reasoning: msg is built by this program. Anything derived from
// client input (a command name, say) must be sanitised before it gets here;
// see server.safeName.
//
// By convention the first word is a machine-readable error code (ERR,
// WRONGTYPE, ...). This function does not add it — the caller passes the whole
// message, code included.
func (w *Writer) WriteError(msg string) error {
	for i := 0; i < len(msg); i++ {
		if msg[i] == '\r' || msg[i] == '\n' {
			panic("resp: CR or LF in error message")
		}
	}
	if _, err := io.WriteString(w.w, "-"); err != nil {
		return err
	}
	if _, err := io.WriteString(w.w, msg); err != nil {
		return err
	}
	_, err := w.w.Write(crlf)
	return err
}

// WriteInteger writes n as a RESP integer: ":1000\r\n".
func (w *Writer) WriteInteger(n int64) error {
	return w.writeCountLine(':', n)
}

// WriteBulkString writes p as a RESP bulk string: "$5\r\nhello\r\n".
//
// The length prefix is a byte count, so the payload is binary-safe: CR, LF and
// NUL all pass through untouched. A nil p is written as an empty bulk string
// ("$0\r\n\r\n"), which is a different value from the null bulk string — use
// WriteNullBulkString for "no value".
func (w *Writer) WriteBulkString(p []byte) error {
	if err := w.writeCountLine('$', int64(len(p))); err != nil {
		return err
	}
	if _, err := w.w.Write(p); err != nil {
		return err
	}
	_, err := w.w.Write(crlf)
	return err
}

// WriteNullBulkString writes the null bulk string "$-1\r\n": RESP2's way of
// saying "no value" (the reply to GET on a missing key).
func (w *Writer) WriteNullBulkString() error {
	_, err := io.WriteString(w.w, "$-1\r\n")
	return err
}

// WriteArrayHeader writes "*<n>\r\n". The caller then writes exactly n values,
// of any types, by calling the other Write methods.
//
// Header-plus-elements rather than one WriteArray([]any) call: the encoder
// never has to know the set of possible value types, and a large reply streams
// out through the buffer instead of being materialised as a []any first.
func (w *Writer) WriteArrayHeader(n int) error {
	return w.writeCountLine('*', int64(n))
}

// WriteNullArray writes the null array "*-1\r\n" (a nil multi-bulk reply,
// e.g. a timed-out BLPOP).
func (w *Writer) WriteNullArray() error {
	_, err := io.WriteString(w.w, "*-1\r\n")
	return err
}

// writeCountLine writes one "<type><int>\r\n" line — the shape shared by
// integers and by the length/count headers of bulk strings and arrays.
//
// The number is formatted with strconv.AppendInt into the Writer's scratch
// array, so encoding a reply costs no heap allocations. 24 bytes holds the type
// byte, any int64 (20 bytes at most, sign included) and CRLF.
func (w *Writer) writeCountLine(typ byte, n int64) error {
	buf := append(w.scratch[:0], typ)
	buf = strconv.AppendInt(buf, n, 10)
	buf = append(buf, crlf...)
	_, err := w.w.Write(buf)
	return err
}
