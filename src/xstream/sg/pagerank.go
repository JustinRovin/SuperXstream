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

type VertexT struct {
	degree uint32
	rank   float32
	sum    float32
}

type UpdateT struct {
	target uint32
	rank   float32
}

type PREngine struct {
	Base BaseEngine
}

func init()