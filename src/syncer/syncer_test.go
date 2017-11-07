package syncer

import (
	"testing"
	"strconv"
	"time"
	"fmt"
	"log"
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
			var rep []byte
			nt := time.Now()
			err := client.Call("localhost:4200", "Worker.DoJob", []byte(strconv.Itoa(i)), &rep)
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

func (w *Worker) DoJob(task []byte, reply *[]byte) error {
	log.Println("Worker: do job", string(task))
	//time.Sleep(time.Second * 3)
	*reply = []byte("OK")
	return nil
}
