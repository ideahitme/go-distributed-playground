/**

End-to-end test for the map reduce operations

*/

package main

import (
	"context"
	"time"

	"github.com/ideahitme/distributed_mit/mapreduce"
)

func main() {
	ctx, _ := context.WithCancel(context.Background())

	master := mapreduce.NewMaster()

	master.StartRPCServer(ctx)
	defer master.CloseMaster()
	master.Add(2)
	for i := 1; i <= 2; i++ {
		worker := mapreduce.NewWorker(i, master.Address)
		go worker.Run()
	}

	go func() {
		time.Sleep(4 * time.Second)
		master.Done()
		time.Sleep(4 * time.Second)
		master.Done()
	}()

	master.Wait()
}
