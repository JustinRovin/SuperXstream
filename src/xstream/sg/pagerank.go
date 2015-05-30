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
	degree       uint32
	rank         float32
	contribution float32
}

type prUpdateT struct {
	target       uint32
	contribution float32
}

type PREngine struct {
	Base BaseEngine

	vertices []prVertexT
+	proceed  bool
}

func (self *PREngine) AllocateVertices() error {
	log.Println("vertexoffset", self.Base.vertexOffset)
 	self.Base.vertexOffset = uint32(self.Base.Partition * self.Base.NumVertices)
 	self.vertices = make([]prVertexT, self.Base.NumVertices)
 
 	return nil
 }

//THis initialization of the verticies should set each
//vert pagerank to 100/(number of verts), so each vert
//get equal pagerankl

func (self *PREngine) Init(phase uint32) error {
	if phase == 0 {
		var startRank float32 = float32(100/self.Base.NumVerticies)

		//we need a mapping for vertices here
		for _, v := range self.Base.NumVertices{
			self.vertices[i] = prVertexT{
				degree: 0, 
				rank: startRank,
				contribution: 0,
			}
		}




		if self.Base.Partition == 0 {
			self.vertices[0].parent = 0
		}

		self.proceed = true
	}

	return nil
}


//for a node, see which out edges point to me
//