package mapreduce

import (
	"net"
	"os"
	"sync"

	"strconv"

	"net/rpc"

	"path/filepath"

	log "github.com/Sirupsen/logrus"
)

type RegisterWorkerArg struct {
	Worker string //address of the worker
}

type Worker struct {
	sync.Mutex
	listener net.Listener
	address  string
	master   string
	tmp      string
}

func NewWorker(id int, master string) *Worker {
	address, err := createUnixSocket("worker-" + strconv.Itoa(id))
	if err != nil {
		log.Fatalln("Unable to create unix socket: ", err)
	}
	return &Worker{address: address, master: master, tmp: filepath.Dir(address)}
}

func (w *Worker) Run() {
	args := RegisterWorkerArg{Worker: w.address}
	var reply SimpleRPCRes
	if ok := rpcCall(w.master, "Master.RPCRegisterWorker", args, &reply); !ok {
		log.Errorf("Failed to register worker: %s", w.address)
	}
	log.Printf("RegisterWorker RPC reply: %s", string(reply))
	w.startRPCServer()
}

func (w *Worker) startRPCServer() {
	server := rpc.NewServer()
	server.Register(w)

	defer w.cleanUp()

	listener, err := net.Listen("unix", w.address)
	if err != nil {
		handleError(err)
	}

	w.listener = listener
	go w.listen(server)
}

func (w *Worker) listen(server *rpc.Server) {
	for {
		conn, err := w.listener.Accept()
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

func (w *Worker) cleanUp() {
	os.Remove(w.tmp)
}

func (w *Worker) RPCShutdownWorker(arg *SimpleRPCArg, res *SimpleRPCRes) error {
	if err := w.listener.Close(); err != nil {
		return err
	}
	log.Printf("Shutting down %s worker", w.address)
	*res = SimpleRPCRes("Success")
	return nil
}
