package yiyidb

import (
	"testing"
	"path/filepath"
	"os"
	"fmt"
	"time"
)

func TestOpenKvdb(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil{
		panic(err)
	}
	// Open/create a queue.
	kv, err := OpenKvdb(dir + "/kvdata")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer kv.Close()

	kv.Put([]byte("hello1"),[]byte("hello value"), 3)
	kv.Put([]byte("hello2"),[]byte("hello value2"), 10)

	all := kv.AllKeys()
	for _, k:= range all{
		fmt.Println(k)
	}

	time.Sleep(5*time.Second)

	all = kv.AllKeys()
	for _, k:= range all{
		fmt.Println(k)
	}

}
