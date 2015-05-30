package netin

import (
	"bytes"
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
	Buffers       []bytes.Buffer
	Gringo        *utils.GringoT
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

	buffers := make([]bytes.Buffer, len(hostInfos))
	for i, _ := range hostInfos {
		buffers[i] = bytes.Buffer{}
	}

	conns := make([]*rpc.Client, len(hostInfos))
	gringo := utils.NewGringo()

	return Host{
		Info:          myHostInfo,
		Buffers:       buffers,
		Gringo:        gringo,
		Partition:     myPartitionIndex,
		PartitionList: hostInfos,
		Connections:   conns,
		EngineType:    config.Engine,
	}
}
