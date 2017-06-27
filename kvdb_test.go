package yiyidb

import (
	"testing"
	"path/filepath"
	"os"
	"fmt"
	"time"
	"strconv"
)

func TestOpenKvdb(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	// Open/create a queue.
	kv, err := OpenKvdb(dir + "/kvdata")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer kv.Close()

	kv.Put([]byte("hello1"), []byte("hello value"), 3)
	kv.Put([]byte("hello2"), []byte("hello value2"), 10)

	kv.SetTTL([]byte("hello2"), 8)

	f, err := kv.GetTTL([]byte("hello2"))
	if err != nil {
		t.Error(err)
	}
	fmt.Println("exp time hello2", f)

	if v, err := kv.Get([]byte("hello1")); err == nil {
		if string(v) != "hello value" {
			t.Error("write error")
		}
	}

	time.Sleep(5 * time.Second)

	_, err = kv.Get([]byte("hello1"))
	if err == nil {
		t.Error("ttl delete error")
	}
}

func TestTtlRunner_Run(t *testing.T) {
	//测试kv数据库及超时TTL服务
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	// Open/create a queue.
	kv, err := OpenKvdb(dir + "/kvdata")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer kv.Close()
	kv.OnExpirse = func(key, value []byte) {
		fmt.Println("exp:", string(key), string(value))
	}

	for i := 0; i <= 10; i++ {
		kv.Put([]byte("hello"+strconv.Itoa(i)), []byte("hello value"+strconv.Itoa(i)), i+1)
	}

	//all := kv.AllKeys()
	//for _, k:= range all{
	//	fmt.Println(k)
	//}
	for i := 1; i < 10; i++ {
		fmt.Println("sleep")
		time.Sleep(3 * time.Second)

		all := kv.AllKeys()
		for _, k := range all {
			fmt.Println(k)
		}
	}

	searchkeys := kv.KeyStart([]byte("hello1"))
	for _, k := range searchkeys {
		fmt.Println(k)
	}

	randkeys := kv.KeyRange([]byte("2017-06-01T01:01:01"), []byte("2017-07-01T01:01:01"))
	for _, k := range randkeys {
		fmt.Println(k)
	}
}
