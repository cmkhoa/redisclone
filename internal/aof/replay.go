package aof

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"redisclone/internal/resp"
)

// ErrCorrupt means the log contains bytes that are not valid RESP anywhere but
// at the very end. Unlike a torn tail, this cannot be explained by a crash
// mid-write, so replay refuses rather than guessing.
var ErrCorrupt = errors.New("aof: corrupt")

// Result reports what a replay did.
type Result struct {
	Commands  int   // commands applied
	Bytes     int64 // bytes of valid log consumed
	Truncated int64 // bytes discarded from a torn tail; 0 if the log was intact
}

// Replay reads the log at path and hands each command to apply, in order.
//
// A missing file is not an error: it is what the first ever startup looks like.
//
// A torn tail — the log ending part-way through a command, which is exactly
// what a crash during a write leaves behind — is truncated away and reported in
// Result.Truncated. The alternative, refusing to start, would mean a power cut
// during an ordinary write leaves an unbootable server; the lost command was by
// definition never acknowledged under any policy stronger than "no".
//
// Corruption anywhere earlier is a different animal (a bad disk, a truncated
// *middle*, someone editing the file) and returns ErrCorrupt without applying
// anything further. Silently skipping it would rebuild a keyspace that never
// existed.
func Replay(path string, apply func(args [][]byte) error) (Result, error) {
	var res Result

	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return res, nil
	}
	if err != nil {
		return res, fmt.Errorf("open aof: %w", err)
	}
	defer f.Close()

	r := resp.NewReader(f)
	for {
		args, err := r.ReadCommand()
		switch {
		case errors.Is(err, io.EOF):
			return res, nil

		case errors.Is(err, io.ErrUnexpectedEOF):
			// Torn tail. res.Bytes is the length of the last complete command,
			// which is where the good prefix ends.
			size, statErr := fileSize(f)
			if statErr != nil {
				return res, statErr
			}
			res.Truncated = size - res.Bytes
			if err := f.Close(); err != nil {
				return res, err
			}
			if err := os.Truncate(path, res.Bytes); err != nil {
				return res, fmt.Errorf("truncate torn aof: %w", err)
			}
			return res, nil

		case err != nil:
			return res, fmt.Errorf("%w: after %d commands (%d bytes): %v",
				ErrCorrupt, res.Commands, res.Bytes, err)
		}

		if len(args) == 0 {
			continue
		}
		if err := apply(args); err != nil {
			return res, fmt.Errorf("%w: command %d (%s): %v",
				ErrCorrupt, res.Commands+1, args[0], err)
		}
		res.Commands++
		// Tracking the good offset by re-computing each command's encoded size
		// avoids having to plumb a byte counter through the RESP reader — the
		// reader buffers ahead, so its position in the file is not the parser's
		// position in the stream. This works because the log is written by our
		// own encoder: the encoding is canonical, so re-deriving its length is
		// exact rather than an estimate.
		res.Bytes += EncodedLen(args)
	}
}

// EncodedLen returns how many bytes Log.Append writes for these arguments.
func EncodedLen(args [][]byte) int64 {
	n := int64(len("*") + len(strconv.Itoa(len(args))) + len("\r\n"))
	for _, a := range args {
		n += int64(len("$") + len(strconv.Itoa(len(a))) + len("\r\n") + len(a) + len("\r\n"))
	}
	return n
}

func fileSize(f *os.File) (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat aof: %w", err)
	}
	return fi.Size(), nil
}
