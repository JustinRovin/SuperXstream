package sg

type prVertexT struct {
	degree uint32
	rank   float32
	sum    float32
}

type prUpdateT struct {
	target uint32
	rank   float32
}

type PREngine struct {
	Base BaseEngine
}

func (self *PREngine) Scatter(phase uint32) error {
	//Unpack parameters
	return nil
}
