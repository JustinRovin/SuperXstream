package sg

import (
	"bytes"
	"encoding/binary"
	//	"fmt"
	"io"
	"log"
	"math"
	"os"
	"unsafe"
	"xstream/utils"

	"github.com/ncw/directio"
)

const dampingFactor float32 = 0.85

type prVertexT struct {
	degree       uint32
	rank         float32
	contribution float32
}

type prUpdateT struct {
	target       uint32
	contribution float32
}

type PREngine struct {
	Base       BaseEngine
	Iterations uint32

	vertices []prVertexT
	proceed  bool
}

func (self *PREngine) AllocateVertices() error {
	self.Base.vertexOffset = uint32(self.Base.Partition * self.Base.NumVertices)
	self.vertices = make([]prVertexT, self.Base.NumVertices)

	return nil
}

func (self *PREngine) ForEachEdge(f func(*PREngine, uint32, uint32,
	[]bytes.Buffer), buffers []bytes.Buffer) error {
	filename := CreateFileName(self.Base.EdgeFile, self.Base.Partition)
	inBlock := directio.AlignedBlock(directio.BlockSize * 3)
	inFile, err := directio.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer inFile.Close()

	var i, x, numBytes int
	var src, dest uint32
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

			if src > 0 || dest > 0 { // ignoring the padding bytes
				f(self, src, dest, buffers)
			}
		}
	}

	return nil
}

func AddEdgeUpdate(self *PREngine, src uint32, dest uint32, buffers []bytes.Buffer) {
	var partition32 uint32 = uint32(self.Base.NumVertices)
	var destPartition uint32 = dest / partition32
	var cont float32 = self.vertices[src-self.Base.vertexOffset].contribution

	//log.Println(cont, " -> ", dest)
	//here "target" (the destination vert) is written
	//then contribution is written (Need to make sure
	//that the buffer is reading out the correct number
	//of bytes when recieved
	buffer := &buffers[destPartition]

	buffer.Write(Uint32bytes(dest))
	buffer.Write(Float32bytes(cont))
}

func InitVert(self *PREngine, src uint32, dest uint32, buffers []bytes.Buffer) {
	self.vertices[src-self.Base.vertexOffset].degree++
}

func (self *PREngine) GetVertices() []byte {
	buffer := new(bytes.Buffer)

	for _, vertex := range self.vertices {
		binary.Write(buffer, binary.LittleEndian, vertex)
	}

	return buffer.Bytes()
}

func (self *PREngine) Scatter(phase uint32, buffers []bytes.Buffer) error {
	if !self.proceed {
		return nil
	}

	self.ForEachEdge(AddEdgeUpdate, buffers)
	return nil
}

func (self *PREngine) Gather(phase uint32, gringo *utils.GringoT,
	numPartitions int) bool {
	self.proceed = false
	self.Iterations--

	//this sets up each vert for summing a new ranking
	for _, v := range self.vertices {
		v.rank = 0
	}

	//this part adds all of the incoming contributions to the
	//destination(target) vertex
	var payload utils.Payload
	var target uint32
	var contribution float32
	var i int
	doneMarkers := 0

	for {
		payload = gringo.Read()
		if payload.Size == 0 {
			doneMarkers++
			if doneMarkers == numPartitions {
				break
			}
		}

		for i = 0; i < payload.Size; i += payload.ObjectSize {
			target = *(*uint32)(unsafe.Pointer(&payload.Bytes[i]))
			contribution = *(*float32)(unsafe.Pointer(&payload.Bytes[i+4]))
			self.vertices[target-self.Base.vertexOffset].rank += contribution
		}
	}

	//this part adds the demping Factor per vertex constant, and
	//multiplies the dampingFactor with the contributions
	//to get the final rank
	perVertDamp := float32((1.0 - dampingFactor) / float32(self.Base.NumVertices))
	for _, v := range self.vertices {
		v.rank = perVertDamp + (dampingFactor * v.rank)
	}

	if self.Iterations > 0 {
		self.proceed = true
	}

	return self.proceed
}

func (self *PREngine) Init(phase uint32) error {
	log.Println("phase: ", phase)
	if phase == 0 {
		var startRank float32 = float32(1) / float32(self.Base.NumVertices)
		log.Println("num verts: ", self.Base.NumVertices)
		log.Println("start Rank: ", startRank)

		for i := range self.vertices {
			v := &self.vertices[i]
			v.degree = 0
			v.rank = startRank
			v.contribution = 0
		}

		self.ForEachEdge(InitVert, nil)

		for i := range self.vertices {
			v := &self.vertices[i]

			if v.degree > 0 {
				v.contribution = float32(v.rank) / float32(v.degree)
			}

			//fmt.Printf("cont %f , rank %f \n", v.contribution, v.rank)
		}

		self.proceed = true
	}

	return nil
}

func Float32frombytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

func Float32bytes(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)
	return bytes
}

func Uint32bytes(value uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, value)
	return bytes
}
