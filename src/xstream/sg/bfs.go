package sg

import (
	"io"
	"log"
	"math"
	"os"
	"unsafe"

	"github.com/ncw/directio"
)

type bfsVertexT struct {
	parent uint32
	phase  uint32
}

type bfsUpdateT struct {
	parent uint32
	child  uint32
}

type BFSEngine struct {
	Base BaseEngine

	vertices  []bfsVertexT
	shouldRun bool
}

func (self *BFSEngine) AllocateVertices() error {
	self.Base.vertexOffset = uint32(self.Base.Partition * self.Base.NumVertices)
	self.vertices = make([]bfsVertexT, self.Base.NumVertices)

	return nil
}

func (self *BFSEngine) Scatter(phase uint32) error {
	filename := CreateFileName(self.Base.EdgeFile, self.Base.Partition)
	inBlock := directio.AlignedBlock(directio.BlockSize * 3)
	inFile, err := directio.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer inFile.Close()

	var partition32 uint32 = uint32(self.Base.NumVertices) // freakin Go
	var i, x, numBytes int
	var src, dest, destPartition uint32
	var vertex *bfsVertexT
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

			if src == 0 && dest == 0 && self.Base.Partition != 0 {
				return nil
			}

			vertex = &self.vertices[src-self.Base.vertexOffset]
			if vertex.parent != math.MaxUint32 && vertex.phase == phase {
				destPartition = dest / partition32
				_ = destPartition
				// Create update
			}
		}
	}
	return nil
}

func (self *BFSEngine) ApplyUpdate(update []byte, phase uint32) bool {
	parent := *(*uint32)(unsafe.Pointer(&update[0]))
	child := *(*uint32)(unsafe.Pointer(&update[4]))
	vertex := &self.vertices[child-self.Base.vertexOffset]

	if vertex.parent == math.MaxUint32 {
		vertex.parent = parent
		vertex.phase = phase
		if !self.shouldRun {
			self.shouldRun = true
		}

		return true
	}

	return false
}

func (self *BFSEngine) Init(phase uint32) bool {
	if phase == 0 {
		for i := 0; i < self.Base.NumVertices; i++ {
			self.vertices[i] = bfsVertexT{parent: math.MaxUint32, phase: 0}
		}

		if self.Base.Partition == 0 {
			self.vertices[0].parent = 0
			return true
		}

		return false
	} else {
		shouldRun := self.shouldRun
		self.shouldRun = false
		return shouldRun
	}
}

func (self *BFSEngine) NeedsEdges() bool {
	return false
}
