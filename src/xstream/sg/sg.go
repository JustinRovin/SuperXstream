package sg

import (
	"bytes"
	"io"
	"log"
	"os"
	"unsafe"
	"xstream/utils"

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

type ScatterGatherEngine interface {
	Init() error
	Scatter() error
	Gather() error
}

// ParseEdges Iterates a given file of edges and writes the edges belonging to
// partition 0 to a new file "|edgeFile|-0". The rest of the edges are sent
// through the channel for the caller to handle.
func ParseEdges(edgeFile string, partitionSize uint32, includeWeights bool,
	gringo *utils.Gringo) {
	inBlock := directio.AlignedBlock(directio.BlockSize * 3)
	outBlock := directio.AlignedBlock(directio.BlockSize)

	inFile, err := directio.OpenFile(edgeFile, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

	defer inFile.Close()
	outFile, err := directio.OpenFile(edgeFile+"-0",
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}

	defer outFile.Close()

	writeBuffer := bytes.NewBuffer(outBlock)
	writeBuffer.Reset()

	var src, dest uint32
	var weight float32
	_, _ = dest, weight // get around unused variable error
	numBytes := 0
	edgeSize := 8
	if includeWeights {
		edgeSize = 12
	}
	edgesPerBlock := directio.BlockSize / edgeSize
	blockEdgeCount := 0
	localEdgeCount := 0

	for err != io.EOF && err != io.ErrUnexpectedEOF {
		var x, y, z int
		x, _ = io.ReadFull(inFile, inBlock[0:directio.BlockSize])
		y, _ = io.ReadFull(inFile,
			inBlock[directio.BlockSize:2*directio.BlockSize])
		z, err = io.ReadFull(inFile,
			inBlock[2*directio.BlockSize:3*directio.BlockSize])
		numBytes = x + y + z

		for i := 0; i < numBytes; i += 12 {
			src = *(*uint32)(unsafe.Pointer(&inBlock[i]))
			dest = *(*uint32)(unsafe.Pointer(&inBlock[i+4]))
			weight = *(*float32)(unsafe.Pointer(&inBlock[i+8]))
			if src < partitionSize {
				writeBuffer.Write(inBlock[i : i+edgeSize])
				// fmt.Printf("Edge %d to %d weight %d", src, dest, weight)

				localEdgeCount++
				blockEdgeCount++
				if blockEdgeCount == edgesPerBlock {
					padBlock(writeBuffer, directio.BlockSize)
					outFile.Write(writeBuffer.Bytes())
					writeBuffer.Reset()
					blockEdgeCount = 0
				}
			} else {
				// send edge to correct host
			}
		}
	}

	if blockEdgeCount > 0 {
		padBlock(writeBuffer, directio.BlockSize)
		outFile.Write(writeBuffer.Bytes())
	}

	log.Println(localEdgeCount, " edges written to disk")
}

func padBlock(buffer *bytes.Buffer, blockSize int) {
	for length := buffer.Len(); length < blockSize; length++ {
		buffer.WriteByte(0)
	}
}
