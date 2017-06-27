# YIYIDB - A fast golang NoSQL database lib for storing big list of data

[![Author](https://img.shields.io/badge/author-@jacoblai-blue.svg?style=flat)](http://www.icoolpy.com/) [![Platform](https://img.shields.io/badge/platform-Linux,%20OpenWrt,%20Android,%20Mac,%20Windows-green.svg?style=flat)](https://github.com/jacoblai/dhdb) [![NoSQL](https://img.shields.io/badge/db-NoSQL-pink.svg?tyle=flat)](https://github.com/jacoblai/dhdb)


YIYIDB is a high performace NoSQL database

## Features

* Pure Go 
* Big data list to 10 billion
* Queue 
* KV list (z-list)
* KV list TTL time expirse auto del and event
* Android or OpenWrt os supported (ARM/MIPS)

## import
```
import "github.com/garyburd/redigo/redis"
```
## KET VALUE LIST

## open or create database
```
dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
if err != nil {
	panic(err)
}

kv, err := yiyidb.OpenKvdb(dir + "/kvdata")
if err != nil {
	fmt.Println(err)
	return
}
defer kv.Close()
```
## reg an event func hook on TTL delete
```
kv.OnExpirse = func(key, value []byte) {
   fmt.Println("exp:", string(key), string(value))
}
```

## insert One key value data
```
kv.Put([]byte("hello1"), []byte("hello value"), 0)
```

## insert One key value data TTL 3 seconds expirse auto delete
```
kv.Put([]byte("hello1"), []byte("hello value"), 0)
```

## set an exists key enable TTL 8 seconds expirse auto delete
```
kv.SetTTL([]byte("hello1"), 8)
```

## get data
```
vaule, err := kv.Get([]byte("hello1"))
if err != nil {
		fmt.Println(err)
}
```

## all keys
```
all := kv.AllKeys()
for _, k := range all {
	fmt.Println(k)
}
```

## keys start with 
```
searchkeys := kv.KeyStart([]byte("hello1"))
for _, k := range searchkeys {
	fmt.Println(k)
}
```

## keys range with
```
randkeys := kv.KeyRange([]byte("2017-06-01T01:01:01"), []byte("2017-07-01T01:01:01"))
for _, k := range randkeys {
	fmt.Println(k)
}
```

## QUEUE LIST

## RPUSH (Qeueu must init new list by select command)
```
//change new list for queue by select command
_, _ = redis.String(c.Do("SELECT", "15"));
myv, _ := redis.Int(c.Do("RPUSH", "foo2","bar3"));
fmt.Println(myv)//print finish count
```

## LPOP (Qeueu must init list queue type)
```
//change new list for queue by select command
_, _ = redis.String(c.Do("SELECT", "15"));
for i := 0; i < 3; i++ {
 av, err := redis.String(c.Do("LPOP"));
 if  err !=nil{
  fmt.Println(err)// EOF Queue
  break;
 }
 fmt.Println("pop finish", i)
 fmt.Println(av)//print pop value
}
```

## Authors

@jacoblai

## Thanks

* syndtr, github.com/syndtr/goleveldb
