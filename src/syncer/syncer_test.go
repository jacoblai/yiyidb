package syncer

import (
	"testing"
	"strconv"
	"fmt"
)

func TestListenRPC(t *testing.T) {
	go ListenRPC()
	N := 1000
	mapChan := make(chan int, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			var rep string
			err := call("localhost", "Worker.DoJob", strconv.Itoa(i), &rep)
			if err != nil{
				t.Error(err)
			}else{
				fmt.Println(i, rep)
			}
			mapChan <- i
		}(i)
	}
	for i := 0; i<N; i++ {
		<-mapChan
	}

}