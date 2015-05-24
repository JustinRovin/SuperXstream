package netin

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
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

	ci := make(chan []byte)
	nc := make(chan string)
	return Host{
		Info:          myHostInfo,
		ByteChannel:   ci,
		NotifyChannel: nc,
		Partition:     myPartitionIndex,
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

func (self *Host) StartInitEdges(edgeSize uint32, file string) {
	//should be go routine
	go sg.InitEdges(self.ByteChannel, self.NotifyChannel, self.Partition, edgeSize, file)
}

func (self *Host) EndInitEdges() {
	self.NotifyChannel <- "done"
}

func (self *Host) AppendInitEdges(bytes []byte) {
	self.ByteChannel <- bytes
}

func SendInitEdge(self *Host, partition int, bytes []byte) {

	if self.Partition == partition {
		self.ByteChannel <- bytes
	} else {
		log.Println("partition number:", partition)
		self.Connections[partition].Call("Host.AppendInitEdges", bytes, nil)
	}
}

func PartitionGraph(self *Host, file string, includeWeights bool) error {
	if self.Partition != 0 {
		return errors.New("Graph processing must be done Host Partition 0")
	}

	edgeSize := 8
	if includeWeights {
		edgeSize = 12
	}

	//Here all of the hosts have their SG engines prepared to recieve edges
	for i, c := range self.Connections {
		if i == self.Partition {
			go sg.InitEdges(self.ByteChannel, self.NotifyChannel, self.Partition, uint32(edgeSize), file)
		} else {
			err := c.Call("Host.StartInitEdges", edgeSize, file)
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
	partitionSize := graphConfig.Graph.Vertices / numPartitions
	if graphConfig.Graph.Vertices%numPartitions > 0 {
		partitionSize++
	}

	log.Println("# Partitions:", numPartitions)
	log.Println("Partition Size:", partitionSize)

	/*
		sg.ParseEdges(file, uint32(numPartitions), uint32(partitionSize),
			false, nil)
	*/

	inBlock := directio.AlignedBlock(directio.BlockSize * 3)
	inFile, err := directio.OpenFile(file, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer inFile.Close()

	byteArr := make([]byte, 4)
	var hostNum, numBytes int
	var src, dest, weight uint32
	_, _ = dest, weight // get around unused variable error

	var i, x int
	for err != io.EOF && err != io.ErrUnexpectedEOF {
		numBytes = 0
		for i = 0; i < 3; i++ {
			x, err = io.ReadFull(inFile,
				inBlock[i*directio.BlockSize:(i+1)*directio.BlockSize])
			numBytes += x
		}

		for i := 0; i < numBytes; i += 8 {
			src = *(*uint32)(unsafe.Pointer(&inBlock[i]))
			dest = *(*uint32)(unsafe.Pointer(&inBlock[i+4]))

			//this should be logic to find host num
			hostNum = 0

			//send the src bytes
			binary.LittleEndian.PutUint32(byteArr, src)
			SendInitEdge(self, hostNum, byteArr)
			//send the dest bytes
			binary.LittleEndian.PutUint32(byteArr, dest)
			SendInitEdge(self, hostNum, byteArr)

			if includeWeights {
				weight = *(*uint32)(unsafe.Pointer(&inBlock[i+8]))
				i += 4

				//send the weight bytes in necessary
				binary.LittleEndian.PutUint32(byteArr, dest)
				SendInitEdge(self, hostNum, byteArr)

			}
		}
	}

	//Here all of the hosts complete the process of recieving edges
	for i, c := range self.Connections {
		if i == self.Partition {
			self.NotifyChannel <- "done"
		} else {
			err := c.Call("Host.EndInitEdges", nil, nil)
			if err != nil {
				fmt.Println("error finishing init edges:", err)
			}
		}
	}

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
