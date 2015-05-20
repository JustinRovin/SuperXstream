package xstream

type Vert struct {

}

type Edge struct {

}

type Update struct {

}

type FastStore struct {
	Verts     Vert[]
	Edge      EdgeList
	OutBuf	  Update[]
	Uout      Update[]
	Uin       Update[]
}

//maybe this slow store isnt necessary? if all the edges fit into fast store? 
//maybe we should just go full main memory until it works to keep it simple?
type SlowStore stuct {
	Indexs    ChunkIndex[]
	Chunks    EdgeList[]
}

type ScatterGatherEngine stuct {
	FastStore  fs
	SlowStore  ss 
}

func InitSGengine...

func Scatter
func Shuffle
func Gather


