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
	Queue         *utils.ScFifo
	Partition     int
	PartitionList []HostInfo
	Connections   []*rpc.Client
	EngineType    string
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
	queue := utils.NewScFifo()

	return Host{
		Info:          myHostInfo,
		Queue:         queue,
		Partition:     myPartitionIndex,
		PartitionList: hostInfos,
		Connections:   conns,
		EngineType:    config.Engine,
	}
}
