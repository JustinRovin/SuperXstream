package netin

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
	"unsafe"
	"xstream/sg"

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
	ByteChannel   chan []byte
	NotifyChannel chan string
	Partition     uint32
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

	ci := make(chan []byte)
	nc := make(chan string)
	return Host{
		Info:          myHostInfo,
		ByteChannel:   ci,
		NotifyChannel: nc,
		Partition:     uint32(myPartitionIndex),
		PartitionList: hostInfos,
		Connections:   conns,
	}
}

/*
func (self *Host) UpdateChannel(vert int, reply *int) error {
	go func() { self.Channel <- vert }()
	fmt.Println("THIS SHIT GOT CALLED YO")
	fmt.Println(<-self.Channel)
	return nil
}
*/

type StartInitEdgesArgs struct {
	EdgeSize int
	File     string
}

func (self *Host) StartInitEdges(args *StartInitEdgesArgs, ack *bool) error {
	//should be go routine
	go sg.InitEdges(self.ByteChannel, self.NotifyChannel, self.Partition, args.EdgeSize, args.File)
	*ack = true
	return nil
}

func (self *Host) EndInitEdges(args bool, ack *bool) error {
	self.NotifyChannel <- "done"
	*ack = true
	return nil
}

func (self *Host) AppendInitEdges(bytes []byte, ack *bool) error {
	self.ByteChannel <- bytes
	*ack = true
	return nil
}

func SendInitEdge(self *Host, partition uint32, bytes []byte) {

	if self.Partition == partition {
		self.ByteChannel <- bytes
	} else {
		var ack bool
		self.Connections[partition].Call("Host.AppendInitEdges", bytes, &ack)
	}
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
		if i == int(self.Partition) {
			go sg.InitEdges(self.ByteChannel, self.NotifyChannel, self.Partition, edgeSize, file)
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
	var hostNum, src, dest, weight uint32
	_, _ = dest, weight // get around unused variable error

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
			dest = *(*uint32)(unsafe.Pointer(&inBlock[i+4]))
			weight = *(*uint32)(unsafe.Pointer(&inBlock[i+8]))

			//this should be logic to find host num
			hostNum = src / partitionSize

			SendInitEdge(self, hostNum, inBlock[i:i+edgeSize])
		}
	}

	//Here all of the hosts complete the process of recieving edges
	for i, c := range self.Connections {
		if i == int(self.Partition) {
			self.NotifyChannel <- "done"
		} else {
			var ack bool
			err := c.Call("Host.EndInitEdges", true, &ack)
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
