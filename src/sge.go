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

type SlowStore stuct {
	Indexs    ChunkIndex[]
	Chunks    EdgeList[]
}


type ScatterGatherEngine stuct {
	FastStore  fs
	SlowStore  ss 
}


