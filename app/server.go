package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	_ "net/http/pprof"
	"os"
	"path/filepath"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	config := initServerConfig(NewServerConfig())
	db, expiryStore, err := SetupServerDbs(config)
	if err != nil {
		os.Exit(2)
	}

	address := getIpV6Address(config)
	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to", address)
		os.Exit(1)
	}
	fmt.Println("listening on", address)

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
	router.Register(KeysCommand)
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
	args := [][]string{
		{"dir", ""},
		{"dbfilename", ""},
		{"port", ""},
	}
	flag.StringVar(&args[0][1], "dir", "/tmp/redis-data", "the directory for redis data files")
	flag.StringVar(&args[1][1], "dbfilename", "dump.rdb", "the name of the db file to write to")
	flag.StringVar(&args[2][1], "port", "6379", "the port to bind this server to")
	flag.Parse()
	return args
}

func SetupServerDbs(config *SharedRWStore[string]) (*SharedRWStore[RespValue], *SharedRWStore[Timestamp], error) {
	dir, _ := config.Get("dir")               // should always exist
	dbfilename, _ := config.Get("dbfilename") // should always exist
	path := filepath.Join(dir, dbfilename)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		fmt.Println("rdb file doesn't exist, starting fresh db instance")
		return NewKVStore(), NewExpiryStore(), nil
	}
	rdbParser, err := NewRDBFileParser(path)
	if err != nil {
		fmt.Println("err initiating rdbfile parser, starting fresh db instance", err)
		return NewKVStore(), NewExpiryStore(), err
	}
	dbs, parseErr := rdbParser.Parse()
	if parseErr != nil {
		fmt.Println("err parsing rdb file, starting fresh db instance", parseErr)
		return NewKVStore(), NewExpiryStore(), parseErr
	}
	return dbs[0].DB, dbs[0].Expiry, nil
}

func getIpV6Address(config *SharedRWStore[string]) string {
	port, _ := config.Get("port")
	return fmt.Sprintf("0.0.0.0:%s", port)
}
