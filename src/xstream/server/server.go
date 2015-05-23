package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"xstream/netin"
)

func Start(host netin.Host) {
	rpc.Register(&host)

	log.Println("Listening on", host.Info.Addr)
	listener, err := net.Listen("tcp", host.Info.Addr)
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	if conn, err := listener.Accept(); err != nil {
		log.Println("accept error: " + err.Error())
	} else {
		go rpc.ServeConn(conn)
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println(os.Args[0], "<config-path> <port>")
		return
	}

	config := netin.LoadConfig(os.Args[1])
	log.Println(config)

	// Setup logging
	if config.Logging == "disable" {
		log.SetOutput(ioutil.Discard)
	}

	host := netin.CreateHost(&config, os.Args[2])
	if host.Partition == 0 {
		log.Println(host.Info.Addr, "is Partition 0. Accepting graph names")
		Start(host)
	} else {
		log.Println(host.Info.Addr, "is waiting for instructions")
		Start(host)
	}
}
