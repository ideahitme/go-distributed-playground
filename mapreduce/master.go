package mapreduce

import (
	"sync"

	"path/filepath"

	"os"

	"net/rpc"

	"net"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// Master is job coordinator
type Master struct {
	sync.WaitGroup
	sync.Mutex
	tmp      string
	Address  string
	listener net.Listener
	shutdown chan struct{}
	workers  []string
}

// NewMaster returns new master object
func NewMaster() *Master {
	address, err := createUnixSocket("master")
	if err != nil {
		log.Fatalln("Unable to create unix socket: ", err)
	}
	return &Master{
		Address:  address,
		tmp:      filepath.Dir(address),
		shutdown: make(chan struct{}),
		workers:  make([]string, 0),
	}
}

// StartRPCServer spins up new RPC Server
func (mr *Master) StartRPCServer(ctx context.Context) {
	server := rpc.NewServer()
	server.Register(mr)

	defer mr.CleanUp()

	listener, err := net.Listen("unix", mr.Address)
	if err != nil {
		handleError(err)
	}

	mr.listener = listener
	go mr.Listen(server)
}

func (mr *Master) Listen(server *rpc.Server) {
	for {
		conn, err := mr.listener.Accept()
		if err != nil {
			handleError(err)
			break
		}
		go func(conn net.Conn) {
			defer conn.Close()
			server.ServeConn(conn)
		}(conn)
	}
}

func (mr *Master) CloseMaster() {
	var reply SimpleRPCRes
	if ok := rpcCall(mr.Address, "Master.RPCShutdown", new(SimpleRPCArg), &reply); !ok {
		log.Error("Failed to shut down Master RPC Server")
	}
	log.Println("CloseMaster RPC call reply: ", reply)
}

func (mr *Master) RPCShutdown(arg *SimpleRPCArg, res *SimpleRPCRes) error {
	log.Println("Got RPC to shut down the master server...")
	close(mr.shutdown)
	if err := mr.listener.Close(); err != nil {
		return err
	}
	for _, worker := range mr.workers {
		rpcCall(worker, "Worker.RPCShutdownWorker", new(SimpleRPCArg), new(SimpleRPCRes))
	}
	*res = SimpleRPCRes("Success")
	return nil
}

func (mr *Master) RPCRegisterWorker(arg *RegisterWorkerArg, res *SimpleRPCRes) error {
	mr.Lock()
	defer mr.Unlock()
	log.Println("Got RPC to register worker: ", arg.Worker)
	mr.workers = append(mr.workers, arg.Worker)
	*res = SimpleRPCRes("Success")
	return nil
}

// CleanUp cleans up all tmp files/dirs
func (mr *Master) CleanUp() {
	os.Remove(mr.tmp)
}
