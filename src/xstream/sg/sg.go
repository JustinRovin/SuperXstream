package sg

import (
	"bytes"
	"log"
	"os"
	"strconv"
	"xstream/utils"

	"github.com/ncw/directio"
)

type ScatterGatherEngine interface {
	AllocateVertices() error
	Init(phase uint32) bool
	Scatter(phase uint32) error

	NeedsEdges() bool
}

type BaseEngine struct {
	EdgeFile    string
	Partition   int
	NumVertices int

	vertexOffset uint32
}

func AppendUpdate(payload *utils.Payload) {

}

//Not sure how you want to do this one, Maybe we can implicitly call
//call this in getOutputPayloads, if so we should remove the call
//from IncrementGatherCount in netin
func ProcessUpdates() {

}

//I think it would be nice if this returned a 2d array of paylaod lists
//refer to the IncrementGatherCount function in netin
//for how I was thinking we would use this
func GetOutputPayloads() [][]*utils.Payload {

	return nil
}

func InitEdges(gringo *utils.GringoT, edgeSize int, edgeFile string) error {
	outBlock := directio.AlignedBlock(directio.BlockSize)
	outFile, err := directio.OpenFile(edgeFile,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)

	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	writeBuffer := bytes.NewBuffer(outBlock)
	writeBuffer.Reset()

	edgesPerBlock := directio.BlockSize / edgeSize
	blockEdgeCount := 0
	diskEdgeCount := 0

	for {
		payload := gringo.Read()
		if payload.Size == 0 {
			break
		}

		for i := 0; i < payload.Size; i += payload.ObjectSize {
			writeBuffer.Write(payload.Bytes[i : i+payload.ObjectSize])
			diskEdgeCount++
			blockEdgeCount++

			if blockEdgeCount == int(edgesPerBlock) {
				padBlock(writeBuffer, directio.BlockSize)
				outFile.Write(writeBuffer.Bytes())
				writeBuffer.Reset()
				blockEdgeCount = 0
			}
		}
	}

	if blockEdgeCount > 0 {
		padBlock(writeBuffer, directio.BlockSize)
		outFile.Write(writeBuffer.Bytes())
	}

	log.Println(diskEdgeCount, "edges written to disk ")
	log.Println("Finished partitioning graph")
	return nil
}

func padBlock(buffer *bytes.Buffer, blockSize int) {
	for length := buffer.Len(); length < blockSize; length++ {
		buffer.WriteByte(0)
	}
}

func CreateFileName(edgeFile string, partition int) string {
	return edgeFile + "-" + strconv.Itoa(partition)
}
