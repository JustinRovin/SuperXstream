package xstream

func InitHost

func Start(host Host) {
	rpc.Register(&host)

	listener, err := net.Listen("tcp", node.Id.Addr)
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