package netin

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"runtime"
	"strings"
)

type Config struct {
	Hosts      []string
	Procs      int
	Logging    string
	Engine     string
	Iterations int
}

func LoadConfig(filename string) (config Config) {
	jsonBlob, err := ioutil.ReadFile(filename)

	if err == nil && json.Unmarshal(jsonBlob, &config) != nil {
		log.Fatal("LoadConfig: ", err)
	}

	if config.Procs < 1 {
		log.Println("LoadConfig: Procs cannot be < 1. Assuming 1.")
	} else if config.Procs > runtime.NumCPU() {
		log.Println("LoadConfig: Procs cannot be > NumCPU. Assuming NumCPU.")
	}
	log.Println("NumCPU:", runtime.NumCPU())

	if config.Logging != "enable" && config.Logging != "disable" {
		log.Println("LoadConfig: Invalid value for 'Logging'. " +
			"Use 'enable' or 'disable'. Assuming 'enable'.")
		config.Logging = "enable"
	}

	return
}

func createHostInfos(hosts []string, myPort string) []HostInfo {
	hostInfos := make([]HostInfo, len(hosts))
	myAddrsRaw, err := net.InterfaceAddrs()

	if err != nil {
		log.Fatal("createHostInfos: ", err)
	}

	// Convert each address to a string and remove the net mask.
	var myAddrs []string
	for _, addr := range myAddrsRaw {
		parts := strings.SplitN(addr.String(), "/", 2)
		myAddrs = append(myAddrs, parts[0])
	}

	for i, hostname := range hosts {
		parts := strings.SplitN(hostname, ":", 2)
		host := parts[0]
		port := parts[1]

		// The host in consideration is this process.
		isMe := false

		if host == "localhost" {
			if myPort == port {
				isMe = true
			}
		} else {
			hostAddrs, err := net.LookupHost(host)

			if err != nil {
				log.Fatal("createHostInfos: ", err)
			}

			if addressesOverlap(myAddrs, hostAddrs) {
				isMe = true
			}
		}

		hostInfos[i] = HostInfo{
			Hostname: host,
			Addr:     host + ":" + port,
			Port:     port,
			Remote:   !isMe,
		}
	}

	return hostInfos
}

func addressesOverlap(fst, snd []string) bool {
	for _, fEntry := range fst {
		for _, sEntry := range snd {
			if fEntry == sEntry {
				return true
			}
		}
	}

	return false
}
