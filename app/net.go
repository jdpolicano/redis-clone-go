package main

import (
	"bytes"
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

const readChunkSize = 1024 // Amount of data to read each time from the io.Reader

// ProtocolReader wraps an io.Reader and uses a ProtoParser to extract complete messages.
// It internally uses a bytes.Buffer to accumulate data between reads.
type ProtocolReader[T any] struct {
	r   io.Reader
	pp  ProtoParser[T]
	buf bytes.Buffer
	err error // persistent error from the underlying reader (if any)
}

// NewProtocolReader returns a new ProtocolReader.
func NewProtocolReader[T any](r io.Reader, pp ProtoParser[T]) *ProtocolReader[T] {
	return &ProtocolReader[T]{
		r:  r,
		pp: pp,
	}
}

// ReadProto attempts to parse and return a complete message of type T from the stream.
func (pr *ProtocolReader[T]) ReadProto() (T, error) {
	var temp [readChunkSize]byte
	for {
		// If a previous read encountered an error and there is no data buffered,
		// then return that error.
		if pr.err != nil && pr.buf.Len() == 0 {
			return *new(T), pr.err
		}

		// Try to parse a message from the current buffer.
		b := pr.buf.Bytes()

		msg, size, parseErr := pr.pp.TryParse(b)
		if parseErr == nil {
			// Successfully parsed a message.
			// Remove the consumed bytes from the buffer.
			pr.buf.Next(size)
			return msg, nil
		} else if parseErr != ErrIncompleteStream {
			// A genuine parsing error occurred.
			return *new(T), parseErr
		}

		n, err := pr.r.Read(temp[:])
		if n > 0 {
			pr.buf.Write(temp[:n])
		}
		if err != nil {
			// Save the error so that if there's no more data to parse, we return it.
			pr.err = err
		}
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
