package mapreduce

import (
	"os"
	"path"
	"strconv"

	"net/rpc"

	log "github.com/Sirupsen/logrus"
)

type SimpleRPCArg string
type SimpleRPCRes string

func createUnixSocket(suffix string) (string, error) {
	tmpPath := path.Join("/var/tmp/", "mapreduce-"+strconv.Itoa(os.Getuid()))
	if _, err := os.Stat(tmpPath); os.IsNotExist(err) {
		os.Mkdir(tmpPath, 0777)
	}

	tmpPath = path.Join(tmpPath, strconv.Itoa(os.Getpid())+"-"+suffix)

	return tmpPath, nil
}

func handleError(err error) {
	if err != nil {
		log.Errorf("Unexpected error: %v", err)
	}
}

func rpcCall(address string, rpcFunc string, args, reply interface{}) bool {
	c, err := rpc.Dial("unix", address)
	if err != nil {
		log.Error(err)
		return false
	}
	defer c.Close()

	if err = c.Call(rpcFunc, args, reply); err != nil {
		log.Error(err)
		return false
	}
	return true
}
