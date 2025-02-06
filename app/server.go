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

	wg := sync.WaitGroup{}
	defer wg.Wait()
	db := NewDatabase("jakes db")
	router := NewCommandRouter()
	router.Register(GetCommand)
	router.Register(SetCommand)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		wg.Add(1)
		go handleConnection(conn, &db, &router, &wg)
	}
}

func handleConnection(conn net.Conn, db *Database, router *CommandRouter, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()
	pp := NewProtocolReader(conn, &RespParser{})
	ctx := NewRequestContext(conn, db)
	for {
		args, err := pp.ReadProto()
		if err != nil {
			return
		}

		if args.Type != Array {
			// to-do handle error here.
			ctx.SendError("args should be array value")
			continue
		}

		router.Route(*ctx, args.Value.([]RespValue))
	}
}
