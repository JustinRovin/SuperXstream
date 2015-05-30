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

func SendUpdatesToHosts(self *Host) error {
	done := make(chan *rpc.Call, len(self.PartitionList))

	for i, buffer := range self.Buffers {
		var ack bool
		if i == self.Partition {
			go func(buffer []byte) {
				self.SendUpdates(buffer, &ack)
				done <- nil
			}(buffer.Bytes())
		} else {
			self.Connections[i].Go("Host.SendUpdates", buffer.Bytes(), &ack,
				done)
		}
	}

	for i := 0; i < len(self.PartitionList); i++ {
		<-done
	}

	for _, buffer := range self.Buffers {
		buffer.Reset()
	}

	return nil
}

func (self *Host) SendUpdates(buffer []byte, ack *bool) error {
	reader := bytes.NewReader(buffer)
	length := reader.Len()

	payload := utils.Payload{Size: 0, ObjectSize: 8}
	bytesRead := 0
	for i := 0; i < length; i += utils.MAX_PAYLOAD_SIZE {
		bytesRead, _ = reader.Read(payload.Bytes[:])
		payload.Size = bytesRead
		self.Gringo.Write(payload)
	}

	payload.Size = 0
	self.Gringo.Write(payload)

	*ack = true
	return nil
}

func RunAlgorithm(self *Host, file string, partitionSize int) error {
	base := &sg.BaseEngine{
		EdgeFile:    file,
		Partition:   0,
		NumVertices: partitionSize,
	}

	startTime := time.Now()

	var ack bool
	for i, conn := range self.Connections {
		base.Partition = i
		if i == self.Partition {
			self.CreateEngine(base, &ack)
		} else {
			conn.Call("Host.CreateEngine", base, &ack)
		}
	}

	done := make(chan *rpc.Call, len(self.PartitionList))
	phase := uint32(0)
	for proceed := true; proceed == true; {
		proceed = false

		for i, conn := range self.Connections {
			if i == self.Partition {
				go func() {
					self.RunPhase(phase, &proceed)
					done <- nil
				}()
			} else {
				conn.Go("Host.RunPhase", phase, &proceed, done)
			}
		}

		for i := 0; i < len(self.PartitionList); i++ {
			<-done
		}

		phase++
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
			conn.Call("Host.GetVertices", 0, &vertices)
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
		self.Info.Engine = &sg.PREngine{Base: *base}
	}

	self.Info.Engine.AllocateVertices()

	*ack = true
	return nil
}

func (self *Host) RunPhase(phase uint32, proceed *bool) error {
	self.Info.Engine.Init(phase)
	self.Info.Engine.Scatter(phase, self.Buffers)
	go SendUpdatesToHosts(self)
	*proceed = self.Info.Engine.Gather(phase+1, self.Gringo,
		len(self.PartitionList))

	return nil
}

func (self *Host) GetVertices(_ int, vertices *GetVerticesResult) error {
	*vertices = GetVerticesResult{Data: self.Info.Engine.GetVertices()}
	return nil
}
