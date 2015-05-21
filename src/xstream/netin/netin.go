package netin

import "net/rpc"
import "fmt"

type HostInfo struct {
	Hostname string
	Addr     string
	Port     string
}

type Host struct {
	Info         HostInfo
	connections  []*rpc.Client
	Channel      chan int
	scatterCount int
}

func (t *Host) UpdateChannel(vert int, something *int) error {
	t.Channel <- vert
	fmt.Println(vert)
	return nil
}

func CreateHost(myPort string) Host {

	//var inf HostInfo
	var conns [3]*rpc.Client

	info := HostInfo{Hostname: myPort, Addr: "localhost", Port: "8080"}
	ci := make(chan int)
	return Host{
		Info:        info,
		connections: conns[:],
		Channel:     ci,
	}
}

/*
func (h *Host) AppendUin(updates *UpdateList, confim *bool) error {
	//this will append updates to the Update In Buffer

}
*/
/*
func (h *Host)
() {
	//this function will increment the scatter count
	//if scatter scount equals the total number of partitions/hosts
	//Gather will be called on the local host

}
*/
