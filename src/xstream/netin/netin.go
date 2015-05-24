package netin

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"xstream/sg"

	"code.google.com/p/gcfg"
)

type HostInfo struct {
	Hostname string
	Addr     string
	Port     string
	Remote   bool
}

type Host struct {
	Info          HostInfo
	Channel       chan int
	Partition     int
	PartitionList []HostInfo
	connections   []*rpc.Client
}

func CreateHost(config *Config, myPort string) Host {
	hostInfos := createHostInfos(config.Hosts, myPort)

	var myHostInfo HostInfo
	var myPartitionIndex int
	for index, host := range hostInfos {
		if host.Remote == false {
			myHostInfo = host
			myPartitionIndex = index
			break
		}
	}

	conns := make([]*rpc.Client, len(hostInfos))

	ci := make(chan int)
	return Host{
		Info:          myHostInfo,
		Channel:       ci,
		Partition:     myPartitionIndex,
		PartitionList: hostInfos,
		connections:   conns,
	}
}

func (self *Host) UpdateChannel(vert int, reply *int) error {
	go func() { self.Channel <- vert }()
	fmt.Println("THIS SHIT GOT CALLED YO")
	fmt.Println(<-self.Channel)
	return nil
}

func PartitionGraph(self *Host, file string) error {
	if self.Partition != 0 {
		return errors.New("Graph processing must be done Host Partition 0")
	}

	var graphConfig sg.GraphConfig
	err := gcfg.ReadFileInto(&graphConfig, file+".ini")
	if err != nil {
		return err
	}

	numPartitions := len(self.PartitionList)
	partitionSize := graphConfig.Graph.Vertices / numPartitions
	if graphConfig.Graph.Vertices%numPartitions > 0 {
		partitionSize++
	}

	log.Println("# Partitions:", numPartitions)
	log.Println("Partition Size:", partitionSize)

	sg.ParseEdges(file, uint32(numPartitions), uint32(partitionSize),
		false, nil)

	log.Println("Finished partitioning graph")
	return nil
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
