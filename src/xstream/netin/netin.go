package netin

import (
	"net/rpc"
	"xstream/sg"
	"xstream/utils"
)

type HostInfo struct {
	Hostname string
	Addr     string
	Port     string
	Remote   bool

	Engine sg.ScatterGatherEngine
}

type Host struct {
	Info          HostInfo
	Gringo        *utils.GringoT
	Partition     int
	PartitionList []HostInfo
	Connections   []*rpc.Client
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
	gringo := utils.NewGringo()

	return Host{
		Info:          myHostInfo,
		Gringo:        gringo,
		Partition:     myPartitionIndex,
		PartitionList: hostInfos,
		Connections:   conns,
	}
}
