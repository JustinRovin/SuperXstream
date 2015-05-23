package main

import (
	"fmt"
	//"log"
	"net"
	"net/rpc"
	"xstream/netin"
	"xstream/sg"
)

func Start(host netin.Host) {
	rpc.Register(&host)

	listener, err := net.Listen("tcp", host.Info.Addr+":"+host.Info.Port)
	if err != nil {
		fmt.Println("Listen error ", err)
		//log.Fatal("listen error: ", err)
	}

	for {
		if conn, err := listener.Accept(); err != nil {
			fmt.Println("accept error: " + err.Error())
		} else {
			go rpc.ServeConn(conn)
		}
	}
}

func main() {
	//here we will init the Host with the SGengine
	//and then start the Host

	sg.ParseEdges("testgraph", 16)

	hostA := netin.CreateHost("A")
	fmt.Println("Starting rcp...")
	Start(hostA)
	fmt.Println("here")
	/*
		arith := new(Arith)
		rpc.Register(arith)
		rpc.HandleHTTP()
		l, e := net.Listen("tcp", ":1234")
		if e != nil {
			log.Fatal("listen error:", e)
		}
		go http.Serve(l, nil)
	*/

}
