package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

var RespEOF = []byte("\r\n")
var ProtoMaxBulkLen = 1 << 10 // 512 mb
var DefautArrayAlloc = 50

type ErrParseUnexpectedType byte

func (e ErrParseUnexpectedType) Error() string {
	return fmt.Sprintf("unexpected protocol type, received: '%s'", string(e))
}

type ErrParseUnterminated struct{}

func (e ErrParseUnterminated) Error() string {
	return "unterminated protocol stream, unrecoverable"
}

type RespParser struct{}

func (rp *RespParser) TryParse(b []byte) (*RespValue, int, error) {
	return deserialize(b)
}

const (
	SimpleString = iota
	SimpleError
	Integer
	BulkString
	NullBulkString
	Array
	NullArray
)

type RespValue struct {
	t int // type, see const enum above
	v any // inner data. based on t, you should be able to safely cast based on 't'
}

func (rv *RespValue) String() string {
	switch rv.t {
	case SimpleString:
		return fmt.Sprintf("SimpleString< %s >", string(rv.v.([]byte)))
	case SimpleError:
		return fmt.Sprintf("SimpleError< %s >", string(rv.v.([]byte)))
	case Integer:
		return fmt.Sprintf("Integer< %d >", rv.v.(int))
	case BulkString:
		return fmt.Sprintf("BulkString< '%s' >", string(rv.v.([]byte)))
	case Array:
		{
			var sb strings.Builder
			sb.WriteString("[")
			for idx, el := range rv.v.([]*RespValue) {
				sb.WriteString(el.String())
				if idx < len(rv.v.([]*RespValue))-1 {
					sb.WriteString(", ")
				}
			}
			sb.WriteString("]")
			return sb.String()
		}
	}
	return "unimplemented"
}

func deserialize(b []byte) (*RespValue, int, error) {
	switch b[0] {
	case '+':
		return simpleString(b)
	case '-':
		return simpleError(b)
	case ':':
		return integer(b)
	case '$':
		return bulkString(b)
	case '*':
		return array(b)
	}
	return nil, 0, nil
}

func simpleString(b []byte) (*RespValue, int, error) {
	endIdx := bytes.Index(b, RespEOF)
	if endIdx < 0 {
		return incomplete()
	}
	inner := bytes.Clone(b[1:endIdx])
	return &RespValue{SimpleString, inner}, endIdx + len(RespEOF), nil
}

func simpleError(b []byte) (*RespValue, int, error) {
	ss, size, err := simpleString(b)
	if err != nil {
		return ss, size, err
	}
	ss.t = SimpleError
	return ss, size, err
}

func integer(b []byte) (*RespValue, int, error) {
	endIdx := bytes.Index(b, RespEOF)
	if endIdx < 0 {
		return incomplete()
	}
	i, e := strconv.Atoi(string(b[1:endIdx]))
	if e != nil {
		return err(e)
	}
	return &RespValue{Integer, i}, endIdx + len(RespEOF), nil
}

func bulkString(b []byte) (*RespValue, int, error) {
	endIdx := bytes.Index(b, RespEOF)
	if endIdx < 0 {
		return incomplete()
	}

	// 1 to cut off the type signature
	msgLen, e := strconv.Atoi(string(b[1:endIdx]))
	if e != nil {
		return err(e)
	}

	if msgLen < 0 {
		return &RespValue{NullBulkString, nil}, endIdx + len(RespEOF), nil
	}

	beginData := endIdx + len(RespEOF)
	endData := beginData + msgLen

	// there should be enough data for the data segment
	if len(b[beginData:]) < msgLen {
		return incomplete()
	}

	// it has to be eof terminated, so first confirm we have the bytes we need...
	if len(b[endData:]) < len(RespEOF) {
		return incomplete()
	}
	// ...then confirm that it is not malformed
	if !isEof(b[endData : endData+len(RespEOF)]) {
		return err(ErrParseUnterminated{})
	}

	data := bytes.Clone(b[beginData:endData])
	return &RespValue{BulkString, data}, endData + len(RespEOF), nil
}

func array(b []byte) (*RespValue, int, error) {

	endIdx := bytes.Index(b, RespEOF)

	if endIdx < 0 {
		return incomplete()
	}

	// 1 to cut off the type signature
	arrLen, e := strconv.Atoi(string(b[1:endIdx]))
	if e != nil {
		return err(e)
	}

	if arrLen < 0 {
		return &RespValue{NullArray, nil}, endIdx + len(RespEOF), nil
	}

	elements := make([]*RespValue, 0, DefautArrayAlloc)
	// the place the parsing last ended
	arrTailIdx := endIdx + len(RespEOF)

	for arrLen > 0 {
		el, size, e := deserialize(b[arrTailIdx:])
		if e != nil {
			return err(e)
		}
		arrTailIdx += size
		arrLen -= 1
		elements = append(elements, el)
	}

	return &RespValue{Array, elements}, arrTailIdx + len(RespEOF), nil
}

func incomplete() (*RespValue, int, error) {
	return nil, 0, ErrIncompleteStream
}

func err(e error) (*RespValue, int, error) {
	return nil, 0, e
}

func isEof(b []byte) bool {
	return bytes.Compare(b, RespEOF) == 0
}
