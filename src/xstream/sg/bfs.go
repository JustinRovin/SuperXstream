package sg

import (
	"bytes"
	"encoding/binary"
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
	Parent uint32
	Child  uint32
}

type BFSEngine struct {
	Base BaseEngine

	vertices []bfsVertexT
}

func (self *BFSEngine) AllocateVertices() error {
	self.Base.vertexOffset = uint32(self.Base.Partition * self.Base.NumVertices)
	self.vertices = make([]bfsVertexT, self.Base.NumVertices)

	log.Println("numpartitions", self.Base.NumPartitions)

	return nil
}

func (self *BFSEngine) GetVertices() []byte {
	buffer := new(bytes.Buffer)

	for _, vertex := range self.vertices {
		binary.Write(buffer, binary.LittleEndian, vertex)
	}

	return buffer.Bytes()
}

func (self *BFSEngine) Scatter(phase uint32, buffers []bytes.Buffer) error {

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

Loop:
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

			if src == math.MaxUint32 && dest == math.MaxUint32 {
				break Loop
			}

			vertex = &self.vertices[src-self.Base.vertexOffset]
			if vertex.parent != math.MaxUint32 && vertex.phase == phase {
				destPartition = dest / partition32
				buffers[destPartition].Write(inBlock[i : i+8])
			}
		}
	}

	log.Println("Scatter completed")
	return nil
}

func (self *BFSEngine) Gather(phase uint32, queue *utils.ScFifo,
	numPartitions int) error {
	doneMarkers := 0

	var payload utils.Payload
	var i int

	var parent, child uint32
	var vertex *bfsVertexT

	if phase == 0 {
		self.Base.Proceed = true
		return nil
	} else {
		self.Base.Proceed = false
	}

	var b bool
	for {
		payload, _ = queue.Dequeue()
		if payload.Size != 0 {
			log.Println("Received payload size", payload.Size)
		}
		if payload.Size == -1 {
			doneMarkers++
			if doneMarkers == numPartitions {
				break
			}
		}
		if b == false {
			// runtime.Gosched()
			// log.Println("EMPTY with doneMarkers", doneMarkers)
		}

		for i = 0; i < payload.Size; i += payload.ObjectSize {
			parent = *(*uint32)(unsafe.Pointer(&payload.Bytes[i]))
			child = *(*uint32)(unsafe.Pointer(&payload.Bytes[i+4]))
			vertex = &self.vertices[child-self.Base.vertexOffset]

			if vertex.parent == math.MaxUint32 {
				vertex.parent = parent
				vertex.phase = phase
				if !self.Base.Proceed {
					self.Base.Proceed = true
				}
			}
		}
	}
	log.Println("Gather completed")

	return nil
}

func (self *BFSEngine) Init(phase uint32) error {
	if phase == 0 {
		for i := 0; i < self.Base.NumVertices; i++ {
			self.vertices[i] = bfsVertexT{parent: math.MaxUint32, phase: 0}
		}

		if self.Base.Partition == 0 {
			self.vertices[0].parent = 0
		}

		self.Base.Proceed = true
	}

	return nil
}

func (self *BFSEngine) Stop() bool {
	return !self.Base.Proceed
}
