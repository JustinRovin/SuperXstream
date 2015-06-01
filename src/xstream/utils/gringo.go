package utils

const MAX_PAYLOAD_SIZE int = 2097152
const growBy int = 8

type Payload struct {
	Size       int
	ObjectSize int
	Bytes      [MAX_PAYLOAD_SIZE]byte
}

type ScFifo struct {
	l      []Payload
	tail   int
	head   int
	length int
	lock   chan int
}

func NewScFifo() *ScFifo {
	q := &ScFifo{}
	q.l = make([]Payload, growBy)
	q.tail = 0
	q.head = 0
	q.length = 0
	q.lock = make(chan int, 1)
	q.lock <- 1
	return q
}

func (q *ScFifo) Enqueue(value Payload) {
	<-q.lock
	if q.length >= len(q.l) {
		q.l = append(q.l[q.tail:], q.l[:q.head]...)
		q.l = append(q.l, make([]Payload, growBy)...)
		q.tail = 0
		q.head = q.length
	}
	q.l[q.head] = value
	q.head = (q.head + 1) % len(q.l)
	q.length += 1
	q.lock <- 1
}

func (q *ScFifo) Dequeue() (Payload, bool) {
	<-q.lock
	if q.length == 0 {
		q.lock <- 1
		return Payload{Size: 0}, false
	}
	value := q.l[q.tail]
	q.tail = (q.tail + 1) % len(q.l)
	q.length -= 1
	q.lock <- 1
	return value, true
}
