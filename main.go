package main

import (
	"fmt"
	"queue"
	"path/filepath"
	"os"
	"os/signal"
)

func main() {
	//dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	//if err != nil{
	//	panic(err)
	//}
	//// Open/create a queue.
	//q, err := yiyidb.OpenQueue(dir + "/data")
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//defer q.Close()

	//// Enqueue an item.
	//item, err := q.Enqueue([]byte("item value"),0)
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}

	//fmt.Println(item.ID)         // 1
	//fmt.Println(item.Key)        // [0 0 0 0 0 0 0 1]
	//fmt.Println(item.Value)      // [105 116 101 109 32 118 97 108 117 101]
	//fmt.Println(item.ToString()) // item value

	//// Change the item value in the queue.
	//item, err = q.Update(item.ID, []byte("new item value"))
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//
	//fmt.Println(item.ToString()) // new item value

	//// Dequeue the next item.
	//deqItem, err := q.Dequeue()
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//
	//fmt.Println(deqItem.ToString()) // new item value

	//// Enqueue an item by 5 seconds ttl time out.
	//_, err = q.Enqueue([]byte("item ttl value"),5)
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//
	//
	//// Enqueue an item by 5 seconds ttl time out.
	//_, err = q.Enqueue([]byte("item ttl value1"),5)
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//
	//q.All()
	//
	//fmt.Println("wait time not out1")
	//time.Sleep(3 * time.Second) //wait for ttl time out
	//
	//// Dequeue the next item.
	//deqItem, err := q.Dequeue()
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//fmt.Println(deqItem.ToString()) // new item value
	//
	//fmt.Println("wait time out2")
	//time.Sleep(3 * time.Second) //wait for ttl time out
	//
	//// Dequeue the next item.
	//deqItem, err = q.Dequeue()
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//fmt.Println(deqItem.ToString()) // new item value
	//
	//// Delete the queue and its database.
	////q.Drop()


	//测试kv数据库及超时TTL服务
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil{
		panic(err)
	}
	// Open/create a queue.
	kv, err := yiyidb.OpenKvdb(dir + "/kvdata")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer kv.Close()
	kv.OnExpirse = OnExp

	kv.Put([]byte("hello1"),[]byte("hello value"), 3)
	kv.Put([]byte("hello2"),[]byte("hello value2"), 10)

	all := kv.AllKeys()
	for _, k:= range all{
		fmt.Println(k)
	}
	//go func() {
	//	for{
	//		fmt.Println("sleep")
	//		time.Sleep(4*time.Second)
	//
	//		all = kv.AllKeys()
	//		for _, k:= range all{
	//			fmt.Println(k)
	//		}
	//	}
	//
	//}()


	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for _ = range signalChan {
			cleanupDone <- true
		}
	}()
	<-cleanupDone
}

func OnExp(key, value []byte) {
	fmt.Println("exp:", string(key), string(value))
}

