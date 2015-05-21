package sg

type ScatterGatherEngine interface {
	Init() error
	Scatter() error
	Gather() error
}
