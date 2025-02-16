package main

import (
	"errors"
	"io"
	"strings"
)

var ErrIncompleteStream = errors.New("stream incomplete, need more data")
var ErrStreamClosed = errors.New("underlying stream has stopped returning data")

// ProtoParser is a generic interface for trying to parse a message from a byte slice.
type ProtoParser[T any] interface {
	// TryParse attempts to parse a message from b.
	// It returns the parsed message, the number of bytes consumed,
	// and an error if parsing failed.
	TryParse([]byte) (T, int, error)
}

const ReadChunkSize = 4096 // Amount of data to read each time from the io.Reader

// ProtocolReader wraps an io.Reader and uses a ProtoParser to extract complete messages.
// It internally uses a bytes.Buffer to accumulate data between reads.
type ProtocolReader[T any] struct {
	r    io.Reader
	pp   ProtoParser[T]
	buf  []byte
	head int
	err  error // persistent error from the underlying reader (if any)
}

// NewProtocolReader returns a new ProtocolReader.
func NewProtocolReader[T any](r io.Reader, pp ProtoParser[T]) *ProtocolReader[T] {
	return &ProtocolReader[T]{
		r:    r,
		pp:   pp,
		buf:  make([]byte, ReadChunkSize),
		head: 0,
		err:  nil,
	}
}

// ReadProto attempts to parse and return a complete message of type T from the stream.
func (pr *ProtocolReader[T]) ReadProto() (T, error) {
	var none T
	for {
		// If a previous read encountered an error and there is no data buffered,
		// then return that error.
		if pr.err != nil && pr.head == 0 {
			return none, pr.err
		}

		// Try to parse a message from the current buffer.
		msg, size, parseErr := pr.pp.TryParse(pr.buf[:pr.head])
		if parseErr == nil {
			// Successfully parsed a message.
			// Remove the consumed bytes from the buffer.
			left := pr.head - size
			if left != 0 {
				copy(pr.buf, pr.buf[size:pr.head])
			}
			pr.head = left
			return msg, nil
		} else if parseErr != ErrIncompleteStream {
			// A genuine parsing error occurred.
			return none, parseErr
		}

		// expand if needed
		if pr.head == len(pr.buf) {
			tmp := make([]byte, (len(pr.buf)*2)+1)
			copy(tmp, pr.buf)
			pr.buf = tmp
		}

		n, err := pr.r.Read(pr.buf[pr.head:])
		pr.head += n
		pr.err = err
	}
}

// ----------------------------------------------------------------------------
// Example Parser Implementation: PingParser
// ----------------------------------------------------------------------------

// PingParser is an example implementation of ProtoParser for a simple "ping\r\n" protocol.
type PingParser struct{}

// TryParse looks for the string "ping\r\n" (case insensitive) in b.
// If found, it returns true, the number of bytes consumed, and a nil error.
// Otherwise, it returns ErrIncompleteStream.
func (pp *PingParser) TryParse(b []byte) (bool, int, error) {
	matcher := "ping\r\n"
	// If the buffer is shorter than the expected matcher, we need more data.
	if len(b) < len(matcher) {
		return false, 0, ErrIncompleteStream
	}
	// Check if the matcher appears in the data (ignoring case).
	idx := strings.LastIndex(strings.ToLower(string(b)), matcher)
	if idx < 0 {
		return false, 0, ErrIncompleteStream
	}
	// Return the parsed value (true), the number of bytes consumed, and no error.
	return true, idx + len(matcher), nil
}

type RespParser struct{}

func (rp RespParser) TryParse(b []byte) (RespValue, int, error) {
	var none RespValue
	if len(b) == 0 {
		return none, 0, ErrIncompleteStream
	}
	return Deserialize(b)
}
