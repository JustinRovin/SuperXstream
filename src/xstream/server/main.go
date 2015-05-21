package main

import (
	"log"
	"net"
	"net/rpc"
)

func Start(host Host) {
	rpc.Register(&host)

	listener, err := net.Listen("tcp", host.HostInfo.Addr)
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	for {
		if conn, err := listener.Accept(); err != nil {
			log.Print("accept error: " + err.Error())
		} else {
			go rpc.ServeConn(conn)
		}
	}
}

func main() {
	//here we will init the Host with the SGengine
	//and then start the Host

}
