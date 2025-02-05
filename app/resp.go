package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

var RespEOF = []byte("\r\n")
var RespEOFLen = len(RespEOF)
var ProtoMaxBulkLen = 1 << 10 // 512 mb
var DefaultArrayAlloc = 50

type ErrDeserializeUnexpectedType byte

func (e ErrDeserializeUnexpectedType) Error() string {
	return fmt.Sprintf("unexpected protocol type, received: '%s'", string(e))
}

type ErrDeserializeUnterminated struct{}

func (e ErrDeserializeUnterminated) Error() string {
	return "unterminated protocol stream, unrecoverable"
}

type ErrSerializeInvalidCharacters string

func (e ErrSerializeInvalidCharacters) Error() string {
	return fmt.Sprintf("serialize invalid characters detected '%s'", string(e))
}

type ErrSerializeUnhandledType int

func (e ErrSerializeUnhandledType) Error() string {
	return fmt.Sprintf("serialize unhanlded type '%d'", e)
}

type RespParser struct{}

func (rp RespParser) TryParse(b []byte) (RespValue, int, error) {
	return Deserialize(b)
}

type RespType int

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
	Type  RespType // type, see const enum above
	Value any      // inner data. based on t, you should be able to safely cast based on 't'
}

func (rv RespValue) String() string {
	switch rv.Type {
	case SimpleString:
	case SimpleError:
	case BulkString:
		return string(rv.Value.([]byte))
	case Integer:
		return fmt.Sprint(rv.Value.(int))
	case Array:
		{
			var sb strings.Builder
			sb.WriteString("[")
			for idx, el := range rv.Value.([]*RespValue) {
				sb.WriteString(el.String())
				if idx < len(rv.Value.([]*RespValue))-1 {
					sb.WriteString(", ")
				}
			}
			sb.WriteString("]")
			return sb.String()
		}
	}
	return "unimplemented"
}

// Serialize a resp value itself...
func (rv RespValue) Serialize() ([]byte, error) {
	if rv.Type == Array {
		// this cast should be safe, but may want to consider doing some validation in the future
		inner := rv.Value.([]RespValue)
		elements := make([][]byte, 0, len(inner))
		for _, el := range inner {
			b, err := el.Serialize()
			if err != nil {
				return nil, err
			}
			elements = append(elements, b)
		}
		return Serialize(Array, elements)
	}

	return Serialize(rv.Type, rv.Value)
}

func (rv RespValue) EqualAsciiInsensitive(s string) bool {
	switch rv.Type {
	case BulkString:
		return bytesEqualsString(rv.Value.([]byte), s)
	case SimpleString:
		return bytesEqualsString(rv.Value.([]byte), s)
	case SimpleError:
		return bytesEqualsString(rv.Value.([]byte), s)
	default:
		return false
	}
}

// compares a byte slice to an ascii string, ignoring case.
func bytesEqualsString(a []byte, b string) bool {
	al := len(a)
	bl := len(b)
	if al != bl {
		return false
	}
	diff := byte('a' - 'A')
	for i := 0; i < al; i++ {
		aChar := a[i]
		bChar := b[i]

		if aChar == bChar {
			continue
		}

		if 'A' <= bChar && bChar <= 'Z' {
			bChar += diff
		} else {
			bChar -= diff
		}

		if aChar != bChar {
			return false
		}
	}
	return true
}

// a generic Serialize function for any byte array with a type T
func Serialize(t RespType, value any) ([]byte, error) {
	switch t {
	case SimpleString:
		cast := value.([]byte)
		return SerializeSimpleString(cast)
	case SimpleError:
		cast := value.([]byte)
		return SerializeSimpleError(cast)
	case Integer:
		cast := value.(int)
		return SerializeInteger(cast), nil
	case NullBulkString:
		return SerializeNullBulkString(), nil
	case BulkString:
		cast := value.([]byte)
		return SerializeBulkString(cast), nil
	case NullArray:
		return SerializeNullArray(), nil
	case Array:
		cast := value.([][]byte)
		arr := make([][]byte, 0, DefaultArrayAlloc)
		for _, subArr := range cast {
			arr = append(arr, subArr)
		}
		return SerializeArray(arr), nil
	}

	return nil, ErrSerializeUnhandledType(t)
}

func SerializeSimpleString(b []byte) ([]byte, error) {
	if bytes.Contains(b, RespEOF) {
		return nil, ErrSerializeInvalidCharacters("\r\n")
	}
	return serializeWithPrefixByte('+', b), nil
}

func SerializeSimpleError(b []byte) ([]byte, error) {
	if bytes.Contains(b, RespEOF) {
		return nil, ErrSerializeInvalidCharacters("\r\n")
	}
	return serializeWithPrefixByte('-', b), nil
}

func SerializeInteger(i int) []byte {
	return serializeWithPrefixByte(':', []byte(strconv.Itoa(i)))
}

func SerializeBulkString(b []byte) []byte {
	numBytes := len(b)
	prefix := serializeWithPrefixByte('$', []byte(strconv.Itoa(numBytes)))
	return serializeWithPrefixSlice(prefix, b)
}

func SerializeNullBulkString() []byte {
	return []byte("$-1\r\n")
}

func SerializeArray(arr [][]byte) []byte {
	numElements := len(arr)
	numElementsStr := strconv.Itoa(numElements)
	prefix := serializeWithPrefixByte('*', []byte(numElementsStr))
	for _, b := range arr {
		prefix = append(prefix, b...)
	}
	return append(prefix, RespEOF...)
}

func SerializeNullArray() []byte {
	return []byte("*-1\r\n")
}

func serializeWithPrefixByte(prefix byte, body []byte) []byte {
	ret := make([]byte, 0, len(body)+RespEOFLen+1)
	ret = append(ret, prefix)
	ret = append(ret, body...)
	return append(ret, RespEOF...)
}

func serializeWithPrefixSlice(prefix []byte, body []byte) []byte {
	ret := make([]byte, 0, len(body)+RespEOFLen+1)
	ret = append(ret, prefix...)
	ret = append(ret, body...)
	return append(ret, RespEOF...)
}

func Deserialize(b []byte) (RespValue, int, error) {
	switch b[0] {
	case '+':
		return DeserializeSimpleString(b)
	case '-':
		return DeserializeSimpleError(b)
	case ':':
		return DeserializeInteger(b)
	case '$':
		return DeserializeBulkString(b)
	case '*':
		return DeserializeArray(b)
	}
	return RespValue{}, 0, nil
}

func DeserializeSimpleString(b []byte) (RespValue, int, error) {
	endIdx := bytes.Index(b, RespEOF)
	if endIdx < 0 {
		return incomplete()
	}
	inner := bytes.Clone(b[1:endIdx])
	return RespValue{SimpleString, inner}, endIdx + RespEOFLen, nil
}

func DeserializeSimpleError(b []byte) (RespValue, int, error) {
	ss, size, err := DeserializeSimpleString(b)
	if err != nil {
		return ss, size, err
	}
	ss.Type = SimpleError
	return ss, size, err
}

func DeserializeInteger(b []byte) (RespValue, int, error) {
	endIdx := bytes.Index(b, RespEOF)
	if endIdx < 0 {
		return incomplete()
	}
	i, e := strconv.Atoi(string(b[1:endIdx]))
	if e != nil {
		return err(e)
	}
	return RespValue{Integer, i}, endIdx + RespEOFLen, nil
}

func DeserializeBulkString(b []byte) (RespValue, int, error) {
	endLenSegment := bytes.Index(b, RespEOF)
	if endLenSegment < 0 {
		return incomplete()
	}
	// 1 to cut off the type signature
	msgLen, e := strconv.Atoi(string(b[1:endLenSegment]))
	if e != nil {
		return err(e)
	}

	if msgLen < 0 {
		return RespValue{NullBulkString, nil}, endLenSegment + RespEOFLen, nil
	}

	beginDataSegment := endLenSegment + RespEOFLen
	endDataSegment := beginDataSegment + msgLen

	// there should be enough data for the data segment and for the trailing '\r\n'
	if len(b[beginDataSegment:]) < msgLen || len(b[endDataSegment:]) < RespEOFLen {
		return incomplete()
	}

	// ...then confirm that it is not malformed
	if !isEof(b[endDataSegment : endDataSegment+RespEOFLen]) {
		return err(ErrDeserializeUnterminated{})
	}

	data := bytes.Clone(b[beginDataSegment:endDataSegment])
	return RespValue{BulkString, data}, endDataSegment + RespEOFLen, nil
}

func DeserializeArray(b []byte) (RespValue, int, error) {
	endLenSegment := bytes.Index(b, RespEOF)

	if endLenSegment < 0 {
		return incomplete()
	}

	// 1 to cut off the type signature
	arrLen, e := strconv.Atoi(string(b[1:endLenSegment]))
	if e != nil {
		return err(e)
	}

	if arrLen < 0 {
		return RespValue{NullArray, nil}, endLenSegment + RespEOFLen, nil
	}

	elements := make([]RespValue, 0, DefaultArrayAlloc)
	// the place the parsing last ended
	arrTailIdx := endLenSegment + RespEOFLen

	for arrLen > 0 {
		el, size, e := Deserialize(b[arrTailIdx:])
		if e != nil {
			return err(e)
		}
		arrTailIdx += size
		arrLen -= 1
		elements = append(elements, el)
	}

	return RespValue{Array, elements}, arrTailIdx, nil
}

func incomplete() (RespValue, int, error) {
	return RespValue{}, 0, ErrIncompleteStream
}

func err(e error) (RespValue, int, error) {
	return RespValue{}, 0, e
}

func isEof(b []byte) bool {
	return bytes.Equal(b, RespEOF)
}
