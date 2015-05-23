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
		var parent, phase int32

		err = binary.Read(f, binary.LittleEndian, &parent)
		err = binary.Read(f, binary.LittleEndian, &phase)

		if count > 0 && parent == 0 && phase == 0 {
			return
		}
		fmt.Printf("%d. %d %d\n", count, parent, phase)
		count += 1
	}
}
