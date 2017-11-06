package syncer

import (
	"testing"
	"strconv"
	"time"
	"fmt"
)

func TestListenRPC(t *testing.T) {
	srv := RpcServer{}
	srv.Register(NewWorker())
	go srv.ListenRPC(":4200")
	N := 1000
	mapChan := make(chan int, N)
	for i := 0; i < N; i++ {
		//go func(i int) {
			client := RpcCleint{}
			var rep string
			nt := time.Now()
			err := client.Call("localhost:4200", "Worker.DoJob", strconv.Itoa(i), &rep)
			if err != nil {
				t.Error(err)
			} else {
				sub := time.Now().Sub(nt)
				fmt.Println(i, rep, sub)
			}
			mapChan <- i
		//}(i)
	}
	for i := 0; i < N; i++ {
		<-mapChan
	}
}

type Worker struct {
	Name string
}

func NewWorker() *Worker {
	return &Worker{"test"}
}

func (w *Worker) DoJob(task string, reply *string) error {
	//log.Println("Worker: do job", task)
	//time.Sleep(time.Second * 3)
	*reply = "OK"
	return nil
}
