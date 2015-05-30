package sg

import (
	"io"
	"log"
	"math"
	"os"
	"unsafe"

	"github.com/ncw/directio"
)


const dampingFactor int = 0.85

type prVertexT struct {
	degree uint32
	rank   float32
	sum    float32
}

type prUpdateT struct {
	target uint32
	rank   float32
}

type PREngine struct {
	Base BaseEngine

	vertices []prVertexT
+	proceed  bool
}

func (self *PREngine) AllocateVertices() error {
	log.Println("vertexoffset", self.Base.vertexOffset)
 	self.Base.vertexOffset = uint32(self.Base.Partition * self.Base.NumVertices)
 	self.vertices = make([]bfsVertexT, self.Base.NumVertices)
 
 	return nil
 }

//THis initialization of the verticies should set each
//vert pagerank to 100/(number of verts), so each vert
//get equal pagerankl

func init()

//for a node, see which out edges point to me
//