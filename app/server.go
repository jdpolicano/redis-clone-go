package main

import (
	"fmt"
	"net"
	"os"
	"sync"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	var wg = sync.WaitGroup{}
	defer wg.Wait()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		wg.Add(1)
		go handleConnection(conn, &wg)
	}
}

func handleConnection(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()
	pp := NewProtocolReader(conn, &RespParser{})
	for {
		el, err := pp.ReadProto()
		if err != nil {
			return
		}

		if el.Type != Array {
			errMsg := "err unexpected type"
			fmt.Println(errMsg, el.Type)
			ret, _ := Serialize(SimpleError, []byte(errMsg))
			conn.Write(ret)
			return
		}

		elements := el.Value.([]RespValue)

		if len(elements) < 1 {
			errMsg := []byte("err expected more arguments")
			fmt.Println(string(errMsg))
			ret, _ := Serialize(SimpleError, errMsg)
			conn.Write(ret)
			continue
		}

		if elements[0].EqualsAsciiInsensitive("ping") {
			s, _ := SerializeSimpleString([]byte("PONG"))
			conn.Write(s)
			continue
		}

		if elements[0].EqualsAsciiInsensitive("echo") {
			if len(elements) < 2 {
				errMsg := []byte("err expected more arguments")
				fmt.Println(string(errMsg))
				ret, _ := Serialize(SimpleError, errMsg)
				conn.Write(ret)
				continue
			}

			echo, err := elements[1].Serialize()
			if err != nil {
				fmt.Println(err)
				return
			}
			conn.Write(echo)
			continue
		}

		errMsg := fmt.Sprintf("unknown command '%s'", elements[0].String())
		fmt.Println(errMsg)
		ret, _ := Serialize(SimpleError, []byte(errMsg))
		conn.Write(ret)
	}
}
