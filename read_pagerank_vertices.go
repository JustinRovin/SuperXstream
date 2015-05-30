package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func main() {
	f, err := os.Open(os.Args[1])
	defer f.Close()

	count := 0
	for err != io.EOF {
		var degree uint32
		var rank, contribution float32

		err = binary.Read(f, binary.LittleEndian, &degree)
		err = binary.Read(f, binary.LittleEndian, &rank)
		err = binary.Read(f, binary.LittleEndian, &contribution)

		fmt.Printf("%d. rank: %f degree: %d cont: %f \n", count, rank, degree, contribution)
		count += 1
	}
}

