package netin

import (
	"xstream/sg"
	"xstream/utils"
)

// func IncrementGatherCount(self *Host) error {
// 	self.GatherCount++

// 	if self.GatherCount == len(self.PartitionList) {
// 		self.GatherCount = 0

// 		//Im thinking we could either call this here, or it could just be called
// 		//implicitly in getOutputPayloads?? what do you think?
// 		//Having this abstracted away from get output payloads could be nice actually,
// 		//because then we could switch up the different algorithms we run on the graph
// 		//pretty easily
// 		sg.ProcessUpdates()

// 		//Im thinking this function "getOutputPayloads" could return a 2d array of payloads
// 		//where the list of update payloads to route will be at the index of the
// 		//partition number it should go to (obviously), then the last payload in that
// 		//list will be of size 0. Then in sendUpdates Ill just blindly send off all the
// 		//payloads
// 		payloads := sg.GetOutputPayloads()
// 		go SendUpdates(self, payloads)
// 	}

// 	return nil
// }

func SendUpdates(self *Host, payloadLists [][]*utils.Payload) error {
	for i, pList := range payloadLists {
		var ack bool
		if i == self.Partition {
			for _, p := range pList {
				self.PushUpdate(p, &ack)
			}
		} else {
			for p := range pList {
				self.Connections[i].Call("PushUpdate", p, &ack)
			}
		}
	}

	return nil
}

func (self *Host) PushUpdate(payload *utils.Payload, ack *bool) error {
	self.Gringo.Write(*payload)

	*ack = true
	return nil
}

func CreateHostEngines(self *Host, file string, partitionSize int) error {
	base := &sg.BaseEngine{
		EdgeFile:    file,
		Partition:   0,
		NumVertices: partitionSize,
	}

	var ack bool
	for i, conn := range self.Connections {
		base.Partition = i
		if i == self.Partition {
			self.CreateEngine(base, &ack)
		} else {
			conn.Call("Host.CreateEngine", base, &ack)
		}
	}

	return nil
}

func (self *Host) CreateEngine(base *sg.BaseEngine, ack *bool) error {
	self.Info.Engine = &sg.BFSEngine{Base: *base}
	self.Info.Engine.AllocateVertices()
	// log.Println("Creating Engine", self.Info.Engine)
	*ack = true
	return nil
}

func CallPhase(self *Host, phase uint32) error {
	return nil
}

func (self *Host) RunPhase(phase *uint32, ack *bool) error {
	if self.Info.Engine.Init(*phase) {
	}

	return nil
}
