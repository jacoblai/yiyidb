package yiyidb

import (
	"testing"
	"path/filepath"
	"os"
	"fmt"
	"time"
	"strconv"
	"io/ioutil"
)

func TestKvdb_KeyRangeByObject(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	kv, err := OpenKvdb(dir+"/kvdata8", false)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	type object struct {
		Value int
	}

	kv.PutObject([]byte("testkey1"), object{1}, 0)
	kv.PutObject([]byte("testkey22"), object{2}, 0)
	kv.PutObject([]byte("testke"), object{3}, 0)

	var o object
	all, err := kv.KeyRangeByObject([]byte("testkey"), []byte("testkey25"), o)
	if err !=nil{
		panic(err)
	}
	for k, v := range all {
		fmt.Println(k, v)
	}

	kv.Drop()
}

func TestKvdb_KeyStartByObject(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	kv, err := OpenKvdb(dir+"/kvdata7", false)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	type object struct {
		Value int
	}

	kv.PutObject([]byte("testkey1"), object{1}, 0)
	kv.PutObject([]byte("testkey2"), object{2}, 0)
	kv.PutObject([]byte("testke"), object{3}, 0)

	var o object
	all, err := kv.KeyStartByObject([]byte("testkey"), o)
	if err != nil {
		panic(err)
	}
	for k, v := range all {
		fmt.Println(k, v)
	}

	kv.Drop()
}

func TestKvdb_AllByKV(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	kv, err := OpenKvdb(dir+"/kvdata6", false)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	kv.Put([]byte("testkey"), []byte("test value1"), 0)
	kv.Put([]byte("testkey1"), []byte("test value2"), 0)

	all := kv.AllByKV()
	for k, v := range all {
		fmt.Println(k, string(v))
	}

	kv.Drop()
}

func TestKvdb_AllByObject(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	kv, err := OpenKvdb(dir+"/kvdata5", false)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	type object struct {
		Value int
	}

	kv.PutObject([]byte("testkey"), object{1}, 0)
	kv.PutObject([]byte("testkey1"), object{2}, 0)

	var o object
	all := kv.AllByObject(o)
	for k, v := range all {
		fmt.Println(k, v)
	}

	kv.Drop()
}

func TestKvdb_Drop(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	kv, err := OpenKvdb(dir+"/kvdata4", false)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	kv.Drop()
}

func TestKvdb_Put(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	kv, err := OpenKvdb(dir+"/kvdata3", false)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	start := time.Now()
	//for i := 0; i < 1000000; i++ {
	ks, err := kv.KeyRange([]byte("key0"), []byte("key1000000"))
	if err != nil {
		panic(err)
	}
	//}
	exp := time.Now().Sub(start)
	fmt.Println(exp, ks)

	kv.Drop()
}

func TestKvdb_BatPutOrDel(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	kv, err := OpenKvdb(dir+"/kvdata2", false)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	items := make([]BatItem, 0)
	//add 50000 items to database
	for i := 1; i < 50000; i++ {
		item := BatItem{
			Op:    "put",
			Ttl:   0,
			Key:   []byte("test" + strconv.Itoa(i)),
			Value: []byte("bat values"),
		}
		items = append(items, item)
	}
	kv.BatPutOrDel(&items)
	last, err := kv.Get([]byte("test9999"))
	if string(last) != "bat values" {
		t.Error("record not put finish")
	}

	//remove 50000 items from database
	for i := 1; i < 50000; i++ {
		item := BatItem{
			Op:    "del",
			Key:   []byte("test" + strconv.Itoa(i)),
			Value: []byte("bat values"),
		}
		items = append(items, item)
	}
	kv.BatPutOrDel(&items)
	_, err = kv.Get([]byte("test9999"))
	if err == nil {
		t.Error("record not del finish")
	}
}

func TestOpenKvdb(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	kv, err := OpenKvdb(dir+"/kvdata1", true)
	if err != nil {
		panic(err)
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

	kv, err := OpenKvdb(dir+"/kvdata", true)
	if err != nil {
		panic(err)
	}
	defer kv.Close()
	kv.OnExpirse = func(key, value []byte) {
		fmt.Println("exp:", string(key), string(value))
	}

	for i := 0; i <= 10; i++ {
		kv.Put([]byte("hello"+strconv.Itoa(i)), []byte("hello value"+strconv.Itoa(i)), i+1)
	}

	for i := 1; i < 10; i++ {
		fmt.Println("sleep")
		time.Sleep(3 * time.Second)

		all := kv.AllKeys()
		for _, k := range all {
			fmt.Println(k)
		}
	}

	searchkeys, _ := kv.KeyStart([]byte("hello1"))
	for _, k := range searchkeys {
		fmt.Println(k)
	}

	randkeys, _ := kv.KeyRange([]byte("2017-06-01T01:01:01"), []byte("2017-07-01T01:01:01"))
	for _, k := range randkeys {
		fmt.Println(k)
	}
}

func TestMaxKeyAndValue(t *testing.T) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	kv, err := OpenKvdb(dir+"/kvdata11", false)
	if err != nil {
		panic(err)
	}

	defer kv.Drop()

	bts, err := ioutil.ReadFile("D:\\GoglandProjects\\yiyidb\\yiyidb.zip")
	if err != nil {
		panic(err)
	}

	err = kv.Put(bts, bts, 0)
	if err != nil {
		panic(err)
	}
	btss, err := kv.Get(bts)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile("D:\\GoglandProjects\\yiyidb\\yiyidb1.zip",btss,777)
	if err != nil {
		panic(err)
	}
	//测试结果显示 key和value不超过512m为保证可用性，但性能下降，建议把key尽可能精练
}
