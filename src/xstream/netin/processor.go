package netin

import (
	"bytes"
	"log"
	"net/rpc"
	"os"
	"time"
	"xstream/sg"
	"xstream/utils"
)

type GetVerticesResult struct {
	Data []byte
}

func (self *Host) AppendUpdates(payload utils.Payload, ack *bool) error {
	self.Queue.Enqueue(payload)
	// log.Println("Appeded payload size", payload.Size)
	return nil
}

func RunAlgorithm(self *Host, file string, partitionSize int) error {
	startTime := time.Now()

	acks := make([]bool, len(self.PartitionList))
	for i, conn := range self.Connections {
		a := &acks[i]
		base := &sg.BaseEngine{
			EdgeFile:      file,
			Partition:     i,
			NumVertices:   partitionSize,
			NumPartitions: len(self.PartitionList),
			TotVertices:   len(self.PartitionList) * partitionSize,
		}

		if i == self.Partition {
			self.CreateEngine(base, a)
		} else {
			conn.Call("Host.CreateEngine", base, a)
		}
	}

	done := make(chan *rpc.Call, len(self.PartitionList))
	phase := uint32(0)
	var ack bool

Loop:
	for {
		for i, conn := range self.Connections {
			if i == self.Partition {
				go func() {
					self.RunInit(phase, &ack)
					done <- nil
				}()
			} else {
				conn.Go("Host.RunInit", phase, &ack, done)
			}
		}
		for i := 0; i < len(self.PartitionList); i++ {
			<-done
		}

		for i, conn := range self.Connections {
			if i == self.Partition {
				go func() {
					self.RunGather(phase, &ack)
					done <- nil
				}()
			} else {
				conn.Go("Host.RunGather", phase, &ack, done)
			}
		}
		for i := 0; i < len(self.PartitionList); i++ {
			<-done
		}

		for i, conn := range self.Connections {
			if i == self.Partition {
				go func() {
					self.RunScatter(phase, &ack)
					done <- nil
				}()
			} else {
				conn.Go("Host.RunScatter", phase, &ack, done)
			}
		}
		for i := 0; i < len(self.PartitionList); i++ {
			<-done
		}

		phase++

		for i, conn := range self.Connections {
			var vote bool
			if i == self.Partition {
				self.Stop(0, &vote)
			} else {
				conn.Call("Host.Stop", 0, &vote)
			}

			if vote {
				break Loop
			}
		}
	}

	log.Println("Phases Run:", phase)
	log.Println("Time elapsed:", time.Since(startTime))

	log.Println("Retrieving Vertices")
	startTime = time.Now()

	outputFile, _ := os.Create("vertices")
	defer outputFile.Close()
	var vertices GetVerticesResult
	for i, conn := range self.Connections {
		if i == self.Partition {
			self.GetVertices(0, &vertices)
			outputFile.Write(vertices.Data)
		} else {
			conn.Call("Host.GetVertices", 1, &vertices)
			outputFile.Write(vertices.Data)
		}
	}

	log.Println("Time elapsed:", time.Since(startTime))
	return nil
}

func (self *Host) CreateEngine(base *sg.BaseEngine, ack *bool) error {
	//put switch here?
	switch self.EngineType {
	case "bfs":
		self.Info.Engine = &sg.BFSEngine{Base: *base}
	case "pagerank":
		self.Info.Engine = &sg.PREngine{Base: *base, Iterations: 3}
		//iteration number is desired# + 2. one for the backwards x-stream gather-scatter cycle and one for setting up the rank?
	}

	self.Info.Engine.AllocateVertices()

	return nil
}

func (self *Host) RunInit(phase uint32, ack *bool) error {
	log.Println("starting phase", phase)
	self.Info.Engine.Init(phase)
	return nil
}

func (self *Host) RunGather(phase uint32, ack *bool) error {
	self.Info.Engine.Gather(phase, self.Queue,
		len(self.PartitionList))
	return nil
}

func (self *Host) RunScatter(phase uint32, ack *bool) error {
	buffers := make([]bytes.Buffer, len(self.PartitionList))
	for i := range buffers {
		buffers[i] = bytes.Buffer{}
	}
	self.Info.Engine.Scatter(phase, buffers)

	var ack2 bool
	for i, b := range buffers {
		length := b.Len()

		payload := utils.Payload{Size: 0, ObjectSize: 8}
		bytesRead := 0
		for j := 0; j < length; j += utils.MAX_PAYLOAD_SIZE {
			bytesRead, _ = b.Read(payload.Bytes[:])
			payload.Size = bytesRead

			if i == self.Partition {
				self.AppendUpdates(payload, &ack2)
			} else {
				self.Connections[i].Call("Host.AppendUpdates", payload, &ack2)
			}
		}

		payload.Size = -1
		if i == self.Partition {
			self.AppendUpdates(payload, &ack2)
		} else {
			self.Connections[i].Call("Host.AppendUpdates", payload, &ack2)
		}
	}

	return nil
}

func (self *Host) GetVertices(_ int, vertices *GetVerticesResult) error {
	*vertices = GetVerticesResult{Data: self.Info.Engine.GetVertices()}
	return nil
}

func (self *Host) Stop(_ int, yes *bool) error {
	*yes = self.Info.Engine.Stop()
	return nil
}
