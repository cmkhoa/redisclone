package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Protocol limits. Both match real Redis, and both are checked *before* any
// allocation is made from the client-supplied number — otherwise "$999999999"
// is a one-line out-of-memory attack.
const (
	// MaxBulkLength is the largest accepted bulk string payload (512 MiB).
	MaxBulkLength = 512 * 1024 * 1024
	// MaxArrayLength is the largest accepted number of arguments in a command.
	MaxArrayLength = 1024 * 1024
	// readerBufSize also bounds how long a single header line may be: a line
	// that doesn't fit is a protocol error rather than unbounded buffering.
	readerBufSize = 16 * 1024
)

// ProtocolError means the client sent bytes that are not valid RESP.
//
// It is deliberately a distinct type from an I/O error, because the two demand
// different handling: an I/O error means the connection is already gone, while
// a protocol error means the connection is alive but the byte stream can no
// longer be trusted — we no longer know where the next command starts. The
// server replies with the message and then closes.
type ProtocolError struct{ msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.msg }

// Message returns the reason without the "Protocol error: " prefix.
func (e *ProtocolError) Message() string { return e.msg }

func protoErr(format string, args ...any) error {
	return &ProtocolError{msg: fmt.Sprintf(format, args...)}
}

// IsProtocolError reports whether err came from malformed client input.
func IsProtocolError(err error) bool {
	var pe *ProtocolError
	return errors.As(err, &pe)
}

// Reader parses RESP commands off a byte stream.
//
// It owns a bufio.Reader: net.Conn.Read hands back arbitrary fragments of the
// stream (half a command, or three of them), and buffering is what turns that
// back into whole commands.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, readerBufSize)}
}

// ReadCommand reads one command: a RESP array of bulk strings, which is how
// every real client sends commands.
//
// It returns the arguments including the command name at index 0. Each
// argument is a freshly allocated []byte that the caller owns — nothing
// aliases the internal buffer, so a handler may hold on to an argument (M1's
// store will) without it being overwritten by the next command.
//
// Returns io.EOF exactly when the client disconnected cleanly at a command
// boundary; io.ErrUnexpectedEOF if it vanished mid-command; a *ProtocolError
// for malformed input.
//
// Inline commands (a bare "PING\r\n" typed into netcat) are not supported: any
// first byte other than '*' is a protocol error. See DECISIONS.md.
func (r *Reader) ReadCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if line[0] != '*' {
		return nil, protoErr("expected '*', got '%s'", printableByte(line[0]))
	}

	n, err := parseInt(line[1:])
	if err != nil || n < 0 {
		return nil, protoErr("invalid multibulk length")
	}
	if n > MaxArrayLength {
		return nil, protoErr("invalid multibulk length")
	}
	if n == 0 {
		// "*0\r\n" is well-formed but empty. Real Redis silently reads the
		// next command; an empty (non-nil) slice lets the caller do the same.
		return [][]byte{}, nil
	}

	args := make([][]byte, n)
	for i := range args {
		if args[i], err = r.readBulkString(); err != nil {
			return nil, err
		}
	}
	return args, nil
}

// readBulkString reads one "$<n>\r\n<n bytes>\r\n" value.
func (r *Reader) readBulkString() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		// Unlike the first line of a command, EOF here is always truncation:
		// the array header promised us another argument.
		return nil, unexpectedEOF(err)
	}
	if line[0] != '$' {
		return nil, protoErr("expected '$', got '%s'", printableByte(line[0]))
	}

	n, err := parseInt(line[1:])
	// A null bulk string ("$-1") is a valid *reply* but never a valid command
	// argument, so every negative length is rejected here.
	if err != nil || n < 0 || n > MaxBulkLength {
		return nil, protoErr("invalid bulk length")
	}

	// Read the payload as exactly n bytes, never line-by-line: the payload may
	// legitimately contain CR and LF. The +2 pulls in the trailing CRLF with
	// the same syscall-level read instead of a second one.
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return nil, unexpectedEOF(err)
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, protoErr("bulk string not terminated by CRLF")
	}
	return buf[:n], nil
}

// readLine reads one CRLF-terminated header line and returns it without the
// CRLF. The returned slice points into the buffer and is only valid until the
// next read — fine, because every caller parses it immediately.
//
// The result is guaranteed non-empty, so callers may index line[0].
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadSlice('\n')
	switch {
	case errors.Is(err, bufio.ErrBufferFull):
		// No LF within readerBufSize bytes. Either garbage or an attempt to
		// make us buffer without limit; either way we cannot resynchronise.
		return nil, protoErr("too big inline request")
	case err != nil && errors.Is(err, io.EOF) && len(line) > 0:
		// EOF partway through a line: the client cut the connection
		// mid-command. A clean EOF with nothing buffered stays io.EOF, which
		// is how the caller recognises a normal disconnect.
		return nil, io.ErrUnexpectedEOF
	case err != nil:
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		// A bare LF. RESP terminators are always both bytes.
		return nil, protoErr("unbalanced newline")
	}
	line = line[:len(line)-2]
	if len(line) == 0 {
		return nil, protoErr("empty line")
	}
	return line, nil
}

// unexpectedEOF promotes a clean EOF to io.ErrUnexpectedEOF, for the positions
// inside a command where "the client hung up here" means truncation rather
// than a normal disconnect.
func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// parseInt parses a RESP length or count. strconv rejects the interesting
// garbage for us ("", "abc", "1 2", "+5", " 3", "1\x00").
func parseInt(b []byte) (int64, error) {
	return strconv.ParseInt(string(b), 10, 64)
}

// printableByte renders a byte for an error message without letting a control
// character from the wire into the reply (errors are CRLF-terminated lines
// with no length prefix, so a stray CR would corrupt the stream).
func printableByte(b byte) string {
	if b >= 0x20 && b < 0x7f {
		return string(b)
	}
	return fmt.Sprintf("\\x%02x", b)
}
