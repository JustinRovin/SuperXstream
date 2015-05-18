package xstream

scatterCount int

type HostInfo struct {
	Hostname string
	Addr     string
	Port     string
}

type Host struct {
	SGeng       ScatterGatherEngine
	Info        HostInfo  
	connections []*rpc.Client
}

func CreateHost(sgeng ScatterGatherEngine, config *Config, myPort string) Host {

	var inf HostInfo
	var conns [3]*rpc.Client

	return Host{
		SGeng:       sgeng,
		Info:        inf,
		connections: conns[:]
	}
}

func (h *Host) AddEdges(edges *EdgeList, confim *bool) error {
	// Validate bucket, key pair.
	valid, key := validateKey(args.Bucket, args.Key)
	if !valid {
		return errors.New("invalid key :" + string(args.Key))
	}

	// Find proper coordinator node.
	position, id := t.PrefList.idForKey(key)
	if id == t.Id {
		t.coordGet(position, key, result)
		return nil
	} else {
		return t.forwardGet(position, args, result)
	}
}



