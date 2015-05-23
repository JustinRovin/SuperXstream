package netin

import (
	"fmt"
	"net/rpc"
)

type HostInfo struct {
	Hostname string
	Addr     string
	Port     string
	Remote   bool
}

type Host struct {
	Info        HostInfo
	Channel     chan int
	Partition   uint32
	connections []*rpc.Client
}

func (t *Host) UpdateChannel(vert int, reply *int) error {
	go func() { t.Channel <- vert }()
	fmt.Println("THIS SHIT GOT CALLED YO")
	fmt.Println(<-t.Channel)
	return nil
}

func CreateHost(config *Config, myPort string) Host {
	hostInfos := createHostInfos(config.Hosts, myPort)

	var myHostInfo HostInfo
	var myPartitionIndex uint32
	for index, host := range hostInfos {
		if host.Remote == false {
			myHostInfo = host
			myPartitionIndex = uint32(index)
			break
		}
	}

	conns := make([]*rpc.Client, len(hostInfos))

	ci := make(chan int)
	return Host{
		Info:        myHostInfo,
		Channel:     ci,
		Partition:   myPartitionIndex,
		connections: conns,
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
