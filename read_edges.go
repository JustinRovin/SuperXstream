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
		var src, dest, weight uint32

		err = binary.Read(f, binary.LittleEndian, &src)
		err = binary.Read(f, binary.LittleEndian, &dest)
		err = binary.Read(f, binary.LittleEndian, &weight)

		fmt.Printf("%d. %d %d %d\n", count, src, dest, weight)
		count += 1
	}
}
