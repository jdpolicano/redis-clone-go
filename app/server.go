package main

import (
	"flag"
	"fmt"
	"net"
	"os"
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

	db := NewKVStore()
	expiryStore := NewExpiryStore()
	config := initServerConfig(NewServerConfig())
	router := initCommandRouter(NewCommandRouter())

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())

			os.Exit(1)
		}
		ctx := NewRequestContext(conn, db, expiryStore, config)
		go handleConnection(conn, router, ctx)
	}
}

func handleConnection(conn net.Conn, router CommandRouter, ctx RequestContext) {
	defer conn.Close()
	pp := NewProtocolReader(conn, &RespParser{})
	for {
		args, err := pp.ReadProto()
		if err != nil {
			return
		}

		if !args.isArray() {
			// to-do handle error here.
			ctx.SendError("args should be array value")
			continue
		}

		router.Route(ctx, args.Value.([]RespValue))
	}
}

func initCommandRouter(router CommandRouter) CommandRouter {
	router.Register(GetCommand)
	router.Register(SetCommand)
	router.Register(EchoCommand)
	router.Register(PingCommand)
	router.Register(ConfigCommand)
	return router
}

func initServerConfig(sc *SharedRWStore[string]) *SharedRWStore[string] {
	opts := parseCliOptions()
	for _, tuple := range opts {
		sc.Set(tuple[0], tuple[1])
	}
	return sc
}

func parseCliOptions() [][]string {
	dir := flag.String("dir", "/tmp/redis-data", "the directory for redis data files")
	dbfilename := flag.String("dbfilename", "dump.rdb", "the name of the db file to write to")
	flag.Parse()
	return [][]string{
		{"dir", *dir},
		{"dbfilename", *dbfilename},
	}
}
