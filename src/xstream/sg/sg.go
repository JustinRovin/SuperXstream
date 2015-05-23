package sg

import (
	"bytes"
	"io"
	"log"
	"os"
	"unsafe"

	"github.com/ncw/directio"
)

type ScatterGatherEngine interface {
	Init() error
	Scatter() error
	Gather() error
}

// ParseEdges Iterates a given file of edges and writes the edges belonging to
// partition 0 to a new file "|edgeFile|-0". The rest of the edges are sent
// through the channel for the caller to handle.
func ParseEdges(edgeFile string, verticesPerPartition uint32) {
	inBlock := directio.AlignedBlock(directio.BlockSize)
	outBlock := directio.AlignedBlock(directio.BlockSize)

	writeBuffer := bytes.NewBuffer(outBlock)
	writeBuffer.Reset()
	var src, dest, weight uint32
	_, _ = dest, weight // get around unused variable error
	numBytes := 0

	in, err := directio.OpenFile(edgeFile, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

	out, err := directio.OpenFile(edgeFile+"-0",
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}

	for err != io.EOF && err != io.ErrUnexpectedEOF {
		numBytes, err = io.ReadFull(in, inBlock)

		for i := 0; i < numBytes; i += 12 {
			src = *(*uint32)(unsafe.Pointer(&inBlock[i]))
			dest = *(*uint32)(unsafe.Pointer(&inBlock[i+4]))
			weight = *(*uint32)(unsafe.Pointer(&inBlock[i+8]))
			if src < verticesPerPartition {
				writeBuffer.Write(inBlock[i : i+12])
				// fmt.Printf("Edge %d to %d weight %d", src, dest, weight)
			} else {
				// send edge to correct host
			}

			if writeBuffer.Len() >= directio.BlockSize {
				out.Write(writeBuffer.Bytes())
				writeBuffer.Reset()
			}
		}
	}

	if writeBuffer.Len() > 0 {
		out.Write(writeBuffer.Bytes())
	}
}
