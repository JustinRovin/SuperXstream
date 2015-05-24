package utils

import (
	"runtime"
	"sync/atomic"
)

// Adapted from https://godoc.org/github.com/textnode/gringo
// Maybe change runtime.Gosched() to busy loop?

// The queue
const queueSize uint64 = 4096
const indexMask uint64 = queueSize - 1

// Should fill 1 cache line
type Payload struct {
	Partition int64
	Bytes     [56]byte
}

// Pad to avoid false sharing
type Gringo struct {
	padding1           [8]uint64
	lastCommittedIndex uint64
	padding2           [8]uint64
	nextFreeIndex      uint64
	padding3           [8]uint64
	readerIndex        uint64
	padding4           [8]uint64
	contents           [queueSize]Payload
	padding5           [8]uint64
}

func NewGringo() *Gringo {
	return &Gringo{lastCommittedIndex: 0, nextFreeIndex: 1, readerIndex: 1}
}

func (self *Gringo) Write(value Payload) {
	var myIndex = atomic.AddUint64(&self.nextFreeIndex, 1) - 1
	// Wait for reader to catch up, so we don't clobber a slot
	// which it is (or will be) reading
	for myIndex > (self.readerIndex + queueSize - 2) {
		runtime.Gosched()
	}
	//Write the item into it's slot
	self.contents[myIndex&indexMask] = value
	//Increment the lastCommittedIndex so the item is available for reading
	for !atomic.CompareAndSwapUint64(&self.lastCommittedIndex, myIndex-1,
		myIndex) {
		runtime.Gosched()
	}
}

func (self *Gringo) Read() Payload {
	var myIndex = atomic.AddUint64(&self.readerIndex, 1) - 1
	//If reader has out-run writer, wait for a value to be committed
	for myIndex > self.lastCommittedIndex {
		runtime.Gosched()
	}
	return self.contents[myIndex&indexMask]
}
