package main

import (
	"fmt"
	"net"
)

// tbd: each command workflow will need to have access to a lot of global state,
// so this will likely exand eventually
type RequestContext struct {
	Connection net.Conn  // the client connection to write to
	DB         *Database // a database ref for reading and writing, must not be copied...
}

func NewRequestContext(conn net.Conn, db *Database) *RequestContext {
	return &RequestContext{conn, db}
}

func (rc RequestContext) SendError(msg string) {
	errMsg, success := Serialize(SimpleError, []byte(msg))
	if success != nil {
		fmt.Println("[err] internal serizliation err, serializing", msg)
	}
	rc.Connection.Write(errMsg)
}

func (rc RequestContext) SendNullBulkString() {
	rc.Connection.Write(SerializeNullBulkString())
	return
}

func (rc RequestContext) SendSimpleString(msg string) {
	res, _ := SerializeSimpleString([]byte(msg))
	rc.Connection.Write(res)
	return
}

func (rc RequestContext) SendResp(v RespValue) {
	payload, err := v.Serialize()
	if err != nil {
		msg := fmt.Sprint("failed to serialize resp", v, err)
		fmt.Println(msg)
		rc.SendError(msg)
		return
	}
	rc.Connection.Write(payload)
	return
}
