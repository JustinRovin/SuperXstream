package sg

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"math"
	"os"

	"github.com/ncw/directio"
	//	"fmt"

	"unsafe"
	"xstream/utils"
)

const dampingFactor float32 = 0.85

type prVertexT struct {
	degree       uint32
	rank         float32
	contribution float32
}

type prUpdateT struct {
	Target uint32
	Rank   float32
}

type PREngine struct {
	Base       BaseEngine
	Iterations int

	// EdgeBlocks []byte

	vertices []prVertexT
}

func (self *PREngine) AllocateVertices() error {
	self.Base.vertexOffset = uint32(self.Base.Partition * self.Base.NumVertices)
	self.vertices = make([]prVertexT, self.Base.NumVertices)

	return nil
}

func (self *PREngine) GetVertices() []byte {
	buffer := new(bytes.Buffer)

	for _, vertex := range self.vertices {
		binary.Write(buffer, binary.LittleEndian, vertex)
	}

	return buffer.Bytes()
}

func (self *PREngine) Scatter(phase uint32, buffers []bytes.Buffer, flusher func(bytes.Buffer, int)) error {

	filename := CreateFileName(self.Base.EdgeFile, self.Base.Partition)
	inBlock := directio.AlignedBlock(directio.BlockSize * 3)
	inFile, err := directio.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer inFile.Close()

	var v *prVertexT
	var i, x, numBytes int
	var src, dest, tmp uint32

	var updateCount int = 0

	var b [4]byte

Loop:
	for numBytes = 1; numBytes > 0; {
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

			v = &self.vertices[src-self.Base.vertexOffset]

			if phase == 0 {
				v.degree++
			} else {
				var destPartition uint32 = dest / uint32(self.Base.NumVertices)
				var rank float32
				if phase == 1 {
					rank = 1.0 / float32(v.degree)
				} else {
					rank = v.rank / float32(v.degree)
				}

				_ = destPartition
				_ = rank

				buffers[destPartition].Write(inBlock[i+4 : i+8])
				tmp = *(*uint32)(unsafe.Pointer(&rank))
				b[0] = byte(tmp)
				b[1] = byte(tmp >> 8)
				b[2] = byte(tmp >> 16)
				b[3] = byte(tmp >> 24)
				buffers[destPartition].Write(b[:])

				updateCount++

				if buffers[destPartition].Len() >= 67108864 {
					flusher(buffers[destPartition], int(destPartition))
					buffers[destPartition] = bytes.Buffer{}
				}
			}
		}
	}

	log.Println(updateCount, " updates generated")

	return nil
}

func (self *PREngine) Gather(phase uint32, queue *utils.ScFifo,
	numPartitions int) error {
	//this part adds all of the incoming contributions to the
	//destination(target) vertex

	if phase == 0 {
		self.Base.Proceed = true
		return nil
	}

	var payload utils.Payload
	var vertex *prVertexT
	var target uint32
	var rank float32
	var i int
	doneMarkers := 0

	var updateCount int = 0

	for {
		payload, _ = queue.Dequeue()
		if payload.Size == -1 {
			doneMarkers++
			if doneMarkers == numPartitions {
				break
			}
		}

		for i = 0; i < payload.Size; i += payload.ObjectSize {
			target = *(*uint32)(unsafe.Pointer(&payload.Bytes[i]))
			rank = *(*float32)(unsafe.Pointer(&payload.Bytes[i+4]))

			vertex = &self.vertices[target-self.Base.vertexOffset]
			vertex.contribution += rank
			vertex.rank = 1 - dampingFactor + dampingFactor*vertex.contribution

			updateCount++
		}
	}

	if phase == uint32(self.Iterations) {
		self.Base.Proceed = false
	} else {
		self.Base.Proceed = true
	}

	log.Println(updateCount, " updates processed")

	return nil
}

func (self *PREngine) Init(phase uint32) error {
	var v *prVertexT

	if phase == 0 {
		for i := range self.vertices {
			self.vertices[i] = prVertexT{degree: 0, rank: 1.0, contribution: 0}
		}
	} else {
		for i := range self.vertices {
			v = &self.vertices[i]
			if phase == 1 {
				v.rank = 1 - dampingFactor
			}

			v.contribution = 0
		}
	}

	// if phase == 0 {
	// 	filename := CreateFileName(self.Base.EdgeFile, self.Base.Partition)
	// 	inBlock := directio.AlignedBlock(directio.BlockSize * 3)
	// 	inFile, err := directio.OpenFile(filename, os.O_RDONLY, 0666)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	defer inFile.Close()

	// 	fi, _ := inFile.Stat()

	// 	self.EdgeBlocks = make([]byte, 0, fi.Size())
	// 	log.Println(int(fi.Size()/(directio.BlockSize*3))+1, "blocks")

	// 	var numBytes, i, x int
	// 	for {
	// 		numBytes = 0
	// 		for i = 0; i < 3; i++ {
	// 			x, err = io.ReadFull(inFile,
	// 				inBlock[i*directio.BlockSize:(i+1)*directio.BlockSize])
	// 			numBytes += x
	// 		}

	// 		if numBytes == 0 {
	// 			break
	// 		}

	// 		self.EdgeBlocks = append(self.EdgeBlocks, inBlock...)
	// 		// self.EdgeBlocks[j] = make([]byte, directio.BlockSize*3)
	// 		// copy(self.EdgeBlocks[j], inBlock)
	// 		// j++
	// 	}
	// }

	self.Base.Proceed = true

	return nil
}

func (self *PREngine) Stop() bool {
	return !self.Base.Proceed
}
