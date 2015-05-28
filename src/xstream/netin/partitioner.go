package netin

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"
	"unsafe"
	"xstream/sg"
	"xstream/utils"

	"code.google.com/p/gcfg"
	"github.com/ncw/directio"
)

type GraphConfig struct {
	Graph struct {
		Name     string
		Type     int
		Vertices int
		Edges    int
	}
}

type StartInitEdgesArgs struct {
	EdgeSize int
	File     string
}

func (self *Host) StartInitEdges(args *StartInitEdgesArgs, ack *bool) error {
	go sg.InitEdges(self.Gringo, args.EdgeSize,
		sg.CreateFileName(args.File, self.Partition))

	*ack = true
	return nil
}

func (self *Host) AppendEdges(payload *utils.Payload, ack *bool) error {
	self.Gringo.Write(*payload)
	*ack = true
	return nil
}

func PartitionGraph(self *Host, file string, includeWeights bool) (int, error) {
	if self.Partition != 0 {
		return 0, errors.New("Graph processing must be done Host Partition 0")
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
			self.StartInitEdges(&args, &ack)
		} else {
			err := c.Call("Host.StartInitEdges", &args, &ack)
			if err != nil {
				fmt.Println("error starting sg initedges:", err)
			}
		}
	}

	var graphConfig GraphConfig

	err := gcfg.ReadFileInto(&graphConfig, file+".ini")
	if err != nil {
		return 0, err
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
				var ack bool
				if partition32 == hostNum {
					self.AppendEdges(payload, &ack)
				} else {
					self.Connections[hostNum].Call("Host.AppendEdges",
						payload, &ack)
				}

				payload.Size = 0
			}
		}
	}

	//Here all of the hosts complete the process of recieving edges
	for i, c := range self.Connections {
		payload = payloads[i]
		var ack bool

		// log.Println("Sending payload size", payload.Size)
		if i == self.Partition {
			self.AppendEdges(payload, &ack)
			payload.Size = 0
			self.AppendEdges(payload, &ack)
		} else {
			err := c.Call("Host.AppendEdges", payload, &ack)
			payload.Size = 0
			err = c.Call("Host.AppendEdges", payload, &ack)
			if err != nil {
				fmt.Println("error finishing init edges:", err)
			}
		}
	}

	log.Println("Time elapsed:", time.Since(startTime))
	return int(partitionSize), nil
}
