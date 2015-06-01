package sg

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"strconv"
	"xstream/utils"

	"github.com/ncw/directio"
)

type ScatterGatherEngine interface {
	AllocateVertices() error
	Init(phase uint32) error
	Scatter(phase uint32, buffers []bytes.Buffer, flusher func(bytes.Buffer, int)) error
	Gather(phase uint32, queue *utils.ScFifo, numPartitions int) error
	GetVertices() []byte
	Stop() bool
}

type BaseEngine struct {
	EdgeFile      string
	Partition     int
	NumVertices   int
	TotVertices   int
	NumPartitions int
	Iterations    int

	Proceed bool

	vertexOffset uint32
}

func InitEdges(queue *utils.ScFifo, edgeSize int, edgeFile string) error {
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

	var payload utils.Payload
	var i int

	for {
		payload, _ = queue.Dequeue()
		if payload.Size == -1 {
			binary.Write(writeBuffer, binary.LittleEndian, int32(-1))
			binary.Write(writeBuffer, binary.LittleEndian, int32(-1))
			blockEdgeCount++
			break
		}

		for i = 0; i < payload.Size; i += payload.ObjectSize {
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
