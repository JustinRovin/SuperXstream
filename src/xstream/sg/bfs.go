package sg

import (
	"io"
	"math"
	"os"

	"github.com/ncw/directio"
)

type vertex struct {
	parent uint32
	phase  uint32
}

type BFSEngine struct {
	GraphPath      string
	PartitionIndex uint32
	NumVertices    uint32
	Channel        chan uint32
	vertexStates   []vertex
	vertexOffset   uint32
	vertexMax      uint32
}

func (bfs *BFSEngine) Init() error {
	bfs.vertexOffset = bfs.PartitionIndex * bfs.NumVertices
	bfs.vertexMax = ((bfs.PartitionIndex + 1) * bfs.NumVertices) - 1

	bfs.vertexStates = make([]vertex, bfs.NumVertices)
	for i := uint32(0); i < bfs.NumVertices; i++ {
		if bfs.PartitionIndex == 0 && i == 0 {
			bfs.vertexStates[i] = vertex{parent: 0, phase: 0}
		} else {
			bfs.vertexStates[i] = vertex{parent: math.MaxUint32, phase: 0}
		}
	}

	return nil
}

func (bfs *BFSEngine) Scatter() error {
	block := directio.AlignedBlock(directio.BlockSize)

	in, _ := directio.OpenFile(bfs.GraphPath, os.O_RDONLY, 0666)
	var bytesRead int
	var err error

	for err != io.EOF && err != io.ErrUnexpectedEOF {
		bytesRead, err = io.ReadFull(in, block)

		for i := 0; i < bytesRead; i++ {

		}
	}

	return nil
}

func (bfs *BFSEngine) Gather() error {

	return nil
}
