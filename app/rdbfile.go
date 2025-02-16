package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"
)

// Byte	Name	        Description
// 0xFF	EOF	            End of the RDB file
// 0xFE	SELECTDB		Database Selector
// 0xFD	EXPIRETIME		Expire time in seconds, see Key Expiry Timestamp
// 0xFC	EXPIRETIMEMS	Expire time in milliseconds, see Key Expiry Timestamp
// 0xFB	RESIZEDB		Hash table sizes for the main keyspace and expires, see Resizedb information
// 0xFA	AUX				Auxiliary fields. Arbitrary key-value settings, see Auxiliary fields
//
// Value Encodings
// 0x00 = String Encoding
// 0x01 = List Encoding
// 0x02 = Set Encoding
// 0x03 = Sorted Set Encoding
// 0x04 = Hash Encoding
// 0x09 = Zipmap Encoding
// 0x0A = Ziplist Encoding
// 0x0B = Intset Encoding
// 0x0C = Sorted Set in Ziplist Encoding
// 0x0D = Hashmap in Ziplist Encoding (Introduced in RDB version 4)
// 0x0E = List in Quicklist encoding (Introduced in RDB version 7)

const (
	// op codes
	EOF             = 0xff
	SelectDB        = 0xfe
	ExpireTimeSec   = 0xfd
	ExpireTimeMilli = 0xfc
	ResizeDb        = 0xfb
	Aux             = 0xfa
	// value types
	StringEnc = 0x00
	// todo: encode the other complex value types...
	// Masks
	LenEnc6Bit    = 0x00 // 00000000
	LenEnc14Bit   = 0x40 // 01000000
	LenEnc32Bit   = 0x80 // 10000000
	LenEncSpecial = 0xc0 // 11000000
	// Magic
	MagicString   = "REDIS"
	VersionStrLen = 4
)

var InvalidHead = errors.New("invlaid header trying to parse rdb dump file")
var ExpectedStringKey = errors.New("encountered unexpected encoding, expected string key")

func isSpecial(l byte) bool {
	return l&LenEncSpecial == LenEncSpecial
}

func is6BitLen(l byte) bool {
	return l&LenEnc6Bit == LenEnc6Bit
}

func is14BitLen(l byte) bool {
	return l&LenEnc14Bit == LenEnc14Bit
}

func is32BitLen(l byte) bool {
	return l&LenEnc32Bit == LenEnc32Bit
}

func SigBits(l byte) byte {
	return byte(l & LenEncSpecial)
}

func InsigBits(l byte) byte {
	return byte(l &^ LenEncSpecial)
}

func isOpCode(n byte) bool {
	switch n {
	case EOF:
	case SelectDB:
	case ExpireTimeSec:

	case ExpireTimeMilli:
	case ResizeDb:
	case Aux:
		return true
	}
	return false
}

func isExpiry(n byte) bool {
	switch n {
	case ExpireTimeSec:
	case ExpireTimeMilli:
		return true
	}
	return false
}

type RDBFileParser struct {
	handle   *os.File
	r        *bufio.Reader
	dbs      []RevivedDB
	selector *RevivedDB
}

type RevivedDB struct {
	DB     *SharedRWStore[RespValue]
	Expiry *SharedRWStore[Timestamp]
}

func NewRevivedDb() RevivedDB {
	return RevivedDB{NewKVStore(), NewExpiryStore()}
}

func NewRDBFileParser(path string) (*RDBFileParser, error) {
	file, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	dbs := make([]RevivedDB, 0)
	return &RDBFileParser{file, bufio.NewReader(file), dbs, nil}, nil
}

func (rdb *RDBFileParser) Parse() ([]RevivedDB, error) {
	headerErr, auxErr := rdb.parseHeader(), rdb.parseAuxFields()
	if headerErr != nil {
		fmt.Println(headerErr)
		return nil, headerErr
	}

	if auxErr != nil {
		fmt.Println(auxErr)
		return nil, auxErr
	}
	rdb.parseBody()
	return rdb.dbs, nil
}

// considered a noop, this will just print some info as it goes for now.
func (rdb *RDBFileParser) parseHeader() error {
	var b [len(MagicString) + VersionStrLen]byte
	_, e := rdb.r.Read(b[:])
	if e != nil {
		fmt.Println(e)
		return e
	}
	fmt.Printf("Header: %s\n", b)
	if !validHeader(b[:]) {
		return InvalidHead
	}
	return nil
}

func (rdb *RDBFileParser) parseAuxFields() error {
	fmt.Println("Aux Fields Begin")
	for {
		next, err := rdb.r.ReadByte()
		if err != nil {
			return err
		}
		if next != Aux {
			rdb.r.UnreadByte()
			fmt.Println("Aux Fields End")
			return nil
		}
		key, keyOk := rdb.parseString()
		value, valueOk := rdb.parseString()

		if keyOk != nil {
			return keyOk
		}

		if valueOk != nil {
			return valueOk
		}

		fmt.Println(key.String(), "=", value.String())
	}
}

func (rdb *RDBFileParser) parseBody() error {
	for {
		opCode, e := rdb.r.ReadByte()
		if e != nil {
			return e
		}
		switch opCode {
		case EOF:
			fmt.Println("eof encountered")
			break
		case SelectDB:
			e := rdb.parseDbSelector()
			if e != nil {
				return e
			}
		case ExpireTimeSec:
			e := rdb.parseExpirySec()
			if e != nil {
				return e
			}
		case ExpireTimeMilli:
			e := rdb.parseExpiryMilliSec()
			if e != nil {
				return e
			}
		case ResizeDb:
			e := rdb.parseResizeDb()
			if e != nil {
				return e
			}
		case Aux:
			return nil
		default:
			k, v, e := rdb.parseKeyValue(opCode)
			if e != nil {
				return e
			}
			rdb.selector.DB.Set(k, v)
		}
	}
}

func (rdb *RDBFileParser) parseExpirySec() error {
	expirySecs, expiryErr := rdb.readUint32(binary.LittleEndian)
	if expiryErr != nil {
		return expiryErr
	}
	expiryTime := time.Unix(int64(expirySecs), 0)
	return rdb.parseExpiryWithTime(expiryTime)
}

func (rdb *RDBFileParser) parseExpiryMilliSec() error {
	expiryMilli, expiryErr := rdb.readUint64(binary.LittleEndian)
	if expiryErr != nil {
		return expiryErr
	}
	expirySecs := int64(expiryMilli / 1000)
	expiryTime := time.Unix(expirySecs, 0)

	return rdb.parseExpiryWithTime(expiryTime)
}

func (rdb *RDBFileParser) parseExpiryWithTime(expiryTime time.Time) error {
	valEnc, valEncErr := rdb.r.ReadByte()
	if valEncErr != nil {
		return valEncErr
	}
	key, value, kvErr := rdb.parseKeyValue(valEnc)
	if kvErr != nil {
		return kvErr
	}
	rdb.selector.Expiry.Set(key, NewTimestampFromExpiry(expiryTime))
	rdb.selector.DB.Set(key, value)
	return nil
}

func (rdb *RDBFileParser) parseKeyValue(valEnc byte) (string, RespValue, error) {
	none := func(e error) (string, RespValue, error) { return "", RespValue{}, e }
	switch valEnc {
	case StringEnc:
		key, keyErr := rdb.parseString()
		if keyErr != nil {
			return none(keyErr)
		}
		value, valErr := rdb.parseString()
		if valErr != nil {
			return none(valErr)
		}
		return key.String(), value, nil
	default:
		return none(errors.New(fmt.Sprintln("unsupported value type:", valEnc)))
	}
}

func (rdb *RDBFileParser) parseDbSelector() error {
	lenEnc, e := rdb.r.ReadByte()
	if e != nil {
		return e
	}
	// the only reason this should error is if its special encoding.
	selector, specErr := rdb.readLengthEncodedInt(lenEnc)
	if specErr != nil {
		return specErr
	}
	fmt.Println("select db#", selector)
	rdb.selectNewDb()
	return nil
}

func (rdb *RDBFileParser) parseResizeDb() error {
	n1, e1 := rdb.r.ReadByte()
	if e1 != nil {
		return e1
	}
	hashLen, hashLenErr := rdb.readLengthEncodedInt(n1)
	if hashLenErr != nil {
		return hashLenErr
	}

	n2, e2 := rdb.r.ReadByte()
	if e2 != nil {
		return e1
	}
	expiryLen, expiryLenErr := rdb.readLengthEncodedInt(n2)
	if expiryLenErr != nil {
		return expiryLenErr
	}
	fmt.Println("skipping db resize, would have made hash", hashLen, "and expiry", expiryLen)
	return nil
}

func (rdb *RDBFileParser) parseString() (RespValue, error) {
	lenEnc, lenErr := rdb.r.ReadByte()
	if lenErr != nil {
		return RespValue{}, lenErr
	}

	if isSpecial(lenEnc) {
		// handle integer types.
		flag := InsigBits(lenEnc)
		return rdb.parseSpecialString(flag)
	}

	return rdb.parseLengthPrefixedString(lenEnc)
}

func (rdb *RDBFileParser) parseLengthPrefixedString(lenEnc byte) (RespValue, error) {
	decodedLen, _ := rdb.readLengthEncodedInt(lenEnc)
	buf := make([]byte, decodedLen)
	_, e := rdb.r.Read(buf)
	return RespValue{BulkString, buf}, e
}

func (rdb *RDBFileParser) parseSpecialString(flag byte) (RespValue, error) {
	// single byte if 6 bits is 0
	var value int
	var err error
	switch flag {
	case 0:
		value, err = rdb.readInt8()
	case 1:
		value, err = rdb.readInt16(binary.LittleEndian)
	case 2:
		value, err = rdb.readInt32(binary.LittleEndian)
	case 3:
	default:
		return RespValue{}, errors.New("compressed strings not implemented yet")
	}
	return RespValue{Integer, value}, err
}

func (rdb *RDBFileParser) readInt8() (int, error) {
	// single byte if 6 bits is 0
	b, e := rdb.r.ReadByte()
	return int(b), e
}

func (rdb *RDBFileParser) readInt16(order binary.ByteOrder) (int, error) {
	// single byte if 6 bits is 0
	var buf [2]byte
	_, e := rdb.r.Read(buf[:])
	if e != nil {
		return 0, e
	}
	var b1, b2 int
	if order == binary.LittleEndian {
		b1, b2 = int(buf[1]), int(buf[0])
	} else {
		b1, b2 = int(buf[0]), int(buf[1])
	}
	return b1<<8 | b2, nil
}

func (rdb *RDBFileParser) readInt32(order binary.ByteOrder) (int, error) {
	var buf [4]byte
	_, e := rdb.r.Read(buf[:])
	if e != nil {
		return 0, e
	}
	var b1, b2, b3, b4 int
	if order == binary.LittleEndian {
		b1, b2, b3, b4 = int(buf[3]), int(buf[2]), int(buf[1]), int(buf[0])
	} else {
		b1, b2, b3, b4 = int(buf[0]), int(buf[1]), int(buf[2]), int(buf[3])
	}
	return b1<<24 | b2<<16 | b3<<8 | b4, nil
}

func (rdb *RDBFileParser) readInt64(order binary.ByteOrder) (int64, error) {
	var buf [8]byte
	_, e := rdb.r.Read(buf[:])
	if e != nil {
		return 0, e
	}
	var b1, b2, b3, b4, b5, b6, b7, b8 int64
	if order == binary.LittleEndian {
		b1, b2, b3, b4, b5, b6, b7, b8 = int64(buf[7]), int64(buf[6]), int64(buf[5]), int64(buf[4]), int64(buf[3]), int64(buf[2]), int64(buf[1]), int64(buf[0])
	} else {
		b1, b2, b3, b4, b5, b6, b7, b8 = int64(buf[0]), int64(buf[1]), int64(buf[2]), int64(buf[3]), int64(buf[4]), int64(buf[5]), int64(buf[6]), int64(buf[7])
	}
	return b1<<56 | b2<<48 | b3<<40 | b4<<32 | b5<<24 | b6<<16 | b7<<8 | b8, nil
}

func (rdb *RDBFileParser) readUint32(order binary.ByteOrder) (uint, error) {
	v, e := rdb.readInt32(order)
	return uint(v), e
}

func (rdb *RDBFileParser) readUint64(order binary.ByteOrder) (uint64, error) {
	v, e := rdb.readInt64(order)
	return uint64(v), e
}

func (rdb *RDBFileParser) readLengthEncodedInt(lenEnc byte) (int, error) {
	if is6BitLen(lenEnc) {
		return int(InsigBits(lenEnc)), nil
	}

	if is14BitLen(lenEnc) {
		rest := int(InsigBits(lenEnc))
		nextByte, err := rdb.readInt8()
		if err != nil {
			return 0, err
		}
		return rest<<8 | nextByte, nil
	}

	if is32BitLen(lenEnc) {
		i, err := rdb.readInt32(binary.BigEndian)
		if err != nil {
			return 0, err
		}
		return i, nil
	}

	return 0, errors.New(fmt.Sprintln("parse error, invalid length encoded value", lenEnc))
}

func (rdb *RDBFileParser) selectNewDb() {
	rdb.dbs = append(rdb.dbs, NewRevivedDb())
	rdb.selector = &rdb.dbs[len(rdb.dbs)-1]
}

func validHeader(header []byte) bool {
	matches, e := regexp.Match("REDIS\\d{4}", header)
	if e != nil {
		fmt.Println(e)
		return false
	}
	if !matches {
		return false
	}
	return true
}
