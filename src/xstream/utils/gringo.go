package utils

const MAX_PAYLOAD_SIZE int = 65504

type Payload struct {
	Size       int
	ObjectSize int
	Bytes      [MAX_PAYLOAD_SIZE]byte
}
