package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
)

type ServerConfig struct {
	Config map[string]string
	Lock   sync.RWMutex
}

func NewServerConfig() *ServerConfig {
	store := make(map[string]string)
	return &ServerConfig{store, sync.RWMutex{}}
}

func (sg *ServerConfig) Set(k string, v string) {
	sg.Lock.Lock()
	defer sg.Lock.Unlock()
	sg.Config[k] = v
}

func (sg *ServerConfig) Get(k string) (string, bool) {
	sg.Lock.RLock()
	defer sg.Lock.RUnlock()
	v, ok := sg.Config[k]
	return v, ok
}

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

	db := NewDatabase()
	config := initServerConfigUnsafe(NewServerConfig())
	router := initCommandRouter()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())

			os.Exit(1)
		}
		go handleConnection(conn, db, router, config)
	}
}

func handleConnection(conn net.Conn, db *Database, router *CommandRouter, config *ServerConfig) {
	defer conn.Close()
	pp := NewProtocolReader(conn, &RespParser{})
	ctx := NewRequestContext(conn, db, config)
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

		router.Route(*ctx, args.Value.([]RespValue))
	}
}

func initCommandRouter() *CommandRouter {
	router := NewCommandRouter()
	router.Register(GetCommand)
	router.Register(SetCommand)
	router.Register(EchoCommand)
	router.Register(PingCommand)
	router.Register(ConfigCommand)
	return router
}

func initServerConfigUnsafe(sc *ServerConfig) *ServerConfig {
	opts := parseCliOptions()
	for _, tuple := range opts {
		sc.Config[tuple[0]] = tuple[1]
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
