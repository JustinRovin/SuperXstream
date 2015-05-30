package sg

import (
	"bytes"
	"io"
	"log"
	"math"
	"os"
	"unsafe"
	"xstream/utils"

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

	vertices []bfsVertexT
	proceed  bool
}

func (self *BFSEngine) AllocateVertices() error {
	log.Println("vertexoffset", self.Base.vertexOffset)
	self.Base.vertexOffset = uint32(self.Base.Partition * self.Base.NumVertices)
	self.vertices = make([]bfsVertexT, self.Base.NumVertices)

	return nil
}

func (self *BFSEngine) Scatter(phase uint32, buffers []bytes.Buffer) error {
	if !self.proceed || (phase == 0 && self.Base.Partition > 0) {
		log.Println("Skipping phase", phase)
		return nil
	}

	filename := CreateFileName(self.Base.EdgeFile, self.Base.Partition)
	inBlock := directio.AlignedBlock(directio.BlockSize * 3)
	inFile, err := directio.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer inFile.Close()

	edgesRead := 0
	updatesCreated := 0

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

			if src > 0 && dest > 0 { // ignoring the padding bytes
				edgesRead++
				log.Println("Edge", src, dest)

				vertex = &self.vertices[src-self.Base.vertexOffset]
				if vertex.parent != math.MaxUint32 && vertex.phase == phase {
					destPartition = dest / partition32
					_ = destPartition
					// Create update
					buffers[destPartition].Write(inBlock[i : i+8])

					updatesCreated++
				}
			}
		}
	}

	log.Println("Phase", phase)
	log.Println("EdgesRead", edgesRead)
	log.Println("UpdatesCreated", updatesCreated)

	return nil
}

func (self *BFSEngine) Gather(phase uint32, gringo *utils.GringoT,
	numPartitions int) bool {
	doneMarkers := 0
	updatesRead := 0
	updatesApplied := 0

	var payload utils.Payload
	var i int

	var parent, child uint32
	var vertex *bfsVertexT

	self.proceed = false

	for {
		payload = gringo.Read()
		if payload.Size == 0 {
			doneMarkers++
			if doneMarkers == numPartitions {
				break
			}
		}

		for i = 0; i < payload.Size; i += payload.ObjectSize {
			parent = *(*uint32)(unsafe.Pointer(&payload.Bytes[i]))
			child = *(*uint32)(unsafe.Pointer(&payload.Bytes[i+4]))
			vertex = &self.vertices[child-self.Base.vertexOffset]

			updatesRead++

			if vertex.parent == math.MaxUint32 {
				vertex.parent = parent
				vertex.phase = phase
				if !self.proceed {
					self.proceed = true
				}

				updatesApplied++
			}
		}
	}

	log.Println("UpdatesRead", updatesRead)
	log.Println("UpdatesApplied", updatesApplied)
	return self.proceed
}

func (self *BFSEngine) Init(phase uint32) error {
	if phase == 0 {
		for i := 0; i < self.Base.NumVertices; i++ {
			self.vertices[i] = bfsVertexT{parent: math.MaxUint32, phase: 0}
		}

		if self.Base.Partition == 0 {
			self.vertices[0].parent = 0
		}

		self.proceed = true
	}

	return nil
}
