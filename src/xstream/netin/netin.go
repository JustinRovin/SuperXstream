package netin

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
	"unsafe"
	"xstream/sg"
	"xstream/utils"

	"code.google.com/p/gcfg"
	"github.com/ncw/directio"
)

type HostInfo struct {
	Hostname string
	Addr     string
	Port     string
	Remote   bool
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

type StartInitEdgesArgs struct {
	EdgeSize int
	File     string
}

func (self *Host) StartInitEdges(args *StartInitEdgesArgs, ack *bool) error {
	go sg.InitEdges(self.Gringo, args.EdgeSize,
		args.File+"-"+strconv.Itoa(self.Partition))

	*ack = true
	return nil
}

func (self *Host) AppendEdges(payload utils.Payload, ack *bool) error {
	self.Gringo.Write(payload)
	*ack = true
	return nil
}

func PartitionGraph(self *Host, file string, includeWeights bool) error {
	if self.Partition != 0 {
		return errors.New("Graph processing must be done Host Partition 0")
	}

	var edgeSize int = 8
	if includeWeights {
		edgeSize = 12
	}

	args := StartInitEdgesArgs{EdgeSize: edgeSize, File: file}
	var ack bool

	//Here all of the hosts have their SG engines prepared to recieve edges
	for i, c := range self.Connections {
		if i == self.Partition {
			go sg.InitEdges(self.Gringo, edgeSize,
				file+"-"+strconv.Itoa(self.Partition))
		} else {
			err := c.Call("Host.StartInitEdges", &args, &ack)
			if err != nil {
				fmt.Println("error starting sg initedges:", err)
			}
		}
	}

	var graphConfig sg.GraphConfig

	err := gcfg.ReadFileInto(&graphConfig, file+".ini")
	if err != nil {
		return err
	}

	numPartitions := len(self.PartitionList)
	partitionSize := uint32(graphConfig.Graph.Vertices / numPartitions)
	if graphConfig.Graph.Vertices%numPartitions > 0 {
		partitionSize++
	}

	log.Println("# Partitions:", numPartitions)
	log.Println("Partition Size:", partitionSize)

	startTime := time.Now()

	inBlock := directio.AlignedBlock(directio.BlockSize * 3)
	inFile, err := directio.OpenFile(file, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer inFile.Close()

	var numBytes int
	var hostNum, src uint32
	var partition32 uint32 = uint32(self.Partition) // freakin Go

	payloads := make([]*utils.Payload, len(self.PartitionList))
	for i := 0; i < len(payloads); i++ {
		payloads[i] = &utils.Payload{Size: 0, ObjectSize: edgeSize}
	}

	var payload *utils.Payload

	var i, x int
	for err != io.EOF && err != io.ErrUnexpectedEOF {
		numBytes = 0
		for i = 0; i < 3; i++ {
			x, err = io.ReadFull(inFile,
				inBlock[i*directio.BlockSize:(i+1)*directio.BlockSize])
			numBytes += x
		}

		for i := 0; i < numBytes; i += 12 {
			src = *(*uint32)(unsafe.Pointer(&inBlock[i]))

			//this should be logic to find host num
			hostNum = src / partitionSize

			payload = payloads[hostNum]
			copy(payload.Bytes[payload.Size:], inBlock[i:i+edgeSize])
			payload.Size += edgeSize
			if payload.Size+edgeSize > utils.MAX_PAYLOAD_SIZE {
				if partition32 == hostNum {
					self.Gringo.Write(*payload)
				} else {
					var ack bool
					self.Connections[hostNum].Call("Host.AppendEdges",
						*payload, &ack)
				}

				payload.Size = 0
			}
		}
	}

	//Here all of the hosts complete the process of recieving edges
	for i, c := range self.Connections {
		payload = payloads[i]

		// log.Println("Sending payload size", payload.Size)
		if i == self.Partition {
			self.Gringo.Write(*payload)
			payload.Size = 0
			self.Gringo.Write(*payload)
		} else {
			var ack bool
			err := c.Call("Host.AppendEdges", *payload, &ack)
			payload.Size = 0
			err = c.Call("Host.AppendEdges", *payload, &ack)
			if err != nil {
				fmt.Println("error finishing init edges:", err)
			}
		}
	}

	log.Println("Finished partitioning graph")
	log.Println("Time elapsed:", time.Since(startTime))
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
