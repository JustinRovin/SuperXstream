package main

import (
	"fmt"
	//"log"
	"net"
	"net/rpc"
	"xstream/netin"
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

func Send(host netin.Host, destHost netin.Host) {
	client, err := rpc.Dial("tcp", destHost.Info.Addr+":"+host.Info.Port)
	if err != nil {
		fmt.Println("dialing:", err)
	}
	var reply int

	err = client.Call("Host.UpdateChannel", 5, &reply)
}

func main() {
	//here we will init the Host with the SGengine
	//and then start the Host

	hostA := netin.CreateHost("A")
	hostB := netin.CreateHost("B")
	fmt.Println("Starting rcp...")
	Start(hostA)
	Send(hostB, hostA)
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
