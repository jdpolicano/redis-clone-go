// This file contains a utility for this program to read from io instances, parse them, and then return the parsed
// data. The idea being, this instance will manage an internal buffer so as to maintain data between runs.

package main

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

const InitMemory = 1 << 12  // 1 kb?
const MinReadBytes = 1 << 8 // this is the minium amount our reader should read in order to decide if it wants to resize

var ErrIncompleteStream = errors.New("stream incomplete, need more data")

var ErrStreamClosed = errors.New("underlying stream has stopped returning data")

type ProtoParser[T any] interface {
	TryParse([]byte) (T, int, error)
}

type ProtocolReader[T any] struct {
	r    io.Reader
	pp   ProtoParser[T]
	head int
	buf  []byte
	err  error
}

func NewProtocolReader[T any](r io.Reader, pp ProtoParser[T]) *ProtocolReader[T] {
	return &ProtocolReader[T]{
		r:    r,
		pp:   pp,
		head: 0,
		buf:  make([]byte, InitMemory),
		err:  nil,
	}
}

func (pr *ProtocolReader[T]) resize() {
	nb := make([]byte, (cap(pr.buf)*2)+1) // we know that the underlying buffer will be non-zero
	copy(nb, pr.buf)
	pr.buf = nb
}

func (pr *ProtocolReader[T]) space() int {
	return cap(pr.buf) - pr.head
}

func (pr *ProtocolReader[T]) shiftLeft(n int) {
	copy(pr.buf, pr.buf[n:])
	pr.head -= n
}

// possible cases I'm trying to reel in here.
// n = 0 err = fatal
// n = 0 err = eof
// n > 0 err = fatal
// n > 0 err = eof
func (pr *ProtocolReader[T]) ReadProto() (T, error) {
	for {
		if pr.err != nil {
			return *new(T), pr.err
		}

		if pr.space() < MinReadBytes {
			pr.resize()
		}
		// attempt to read
		n, readErr := pr.r.Read(pr.buf[pr.head:])
		pr.err = readErr
		if n > 0 {
			pr.head += n
			proto, size, parseErr := pr.pp.TryParse(pr.buf[:pr.head])
			if parseErr != nil {
				if parseErr != ErrIncompleteStream {
					return *new(T), parseErr
				}
			} else {
				pr.shiftLeft(size)
				return proto, nil
			}
		}
	}
}

// TODO remove this when we start passing the ping stages...
type PingParser struct{}

func (pp *PingParser) TryParse(b []byte) (bool, int, error) {
	fmt.Println("begin...", string(b), "...end")
	matcher := "ping\r\n"
	idx := strings.LastIndex(string(b), matcher)
	if idx < 0 {
		return false, 0, ErrIncompleteStream
	}
	return true, idx + len(matcher), nil
}
