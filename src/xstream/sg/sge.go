package sg

type ScatterGatherEngine interface {
	Init(string, uint32, chan uint32) error
	Scatter() error
	Gather() error
}
