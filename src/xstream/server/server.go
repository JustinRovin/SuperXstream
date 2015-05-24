package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"runtime"
	"xstream/netin"
)

func Start(host netin.Host) {
	rpc.Register(&host)

	log.Println("Listening on", host.Info.Addr)
	listener, err := net.Listen("tcp", host.Info.Addr)
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	for {
		if conn, err := listener.Accept(); err != nil {
			log.Println("accept error: " + err.Error())
		} else {
			go rpc.ServeConn(conn)
		}
	}
}

func main() {
	if len(os.Args) != 4 {
		fmt.Println(os.Args[0], "<config-path> <port> <graph-path>")
		return
	}

	config := netin.LoadConfig(os.Args[1])
	log.Println(config)

	// Allow multiple cores/threads
	runtime.GOMAXPROCS(config.Procs)

	// Setup logging
	if config.Logging == "disable" {
		log.SetOutput(ioutil.Discard)
	}

	host := netin.CreateHost(&config, os.Args[2])

	if host.Partition == 0 {
		go Start(host)

		log.Println(host.Info.Addr, "is Partition 0.")
		log.Println(host.Info.Addr, "is processing graph", os.Args[3])
		err := netin.PartitionGraph(&host, os.Args[3])
		if err != nil {
			log.Fatal("PartitionGraph: ", err)
		}
	} else {
		log.Println(host.Info.Addr, "is waiting for instructions")
		Start(host)
	}
}
