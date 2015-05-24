package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"unsafe"

	"github.com/ncw/directio"
)

func main() {
	inBlock := directio.AlignedBlock(directio.BlockSize)
	inFile, err := directio.OpenFile(os.Args[1], os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

	defer inFile.Close()

	var src, dest uint32
	var weight float32
	numBytes := 0

	count := 0

	for err != io.EOF && err != io.ErrUnexpectedEOF {
		numBytes, err = io.ReadFull(inFile, inBlock)

		for i := 0; i < numBytes-4; i += 12 {
			src = *(*uint32)(unsafe.Pointer(&inBlock[i]))
			dest = *(*uint32)(unsafe.Pointer(&inBlock[i+4]))
			weight = *(*float32)(unsafe.Pointer(&inBlock[i+8]))
			fmt.Printf("%d. %d %d %f\n", count, src, dest, weight)
			count += 1

		}
	}
}
