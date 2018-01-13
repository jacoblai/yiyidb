# YIYIDB 高性能no-sql数据库

[![Author](https://img.shields.io/badge/author-@jacoblai-blue.svg?style=flat)](http://www.icoolpy.com/) [![Platform](https://img.shields.io/badge/platform-Linux,%20OpenWrt,%20Android,%20Mac,%20Windows-green.svg?style=flat)](https://github.com/jacoblai/dhdb) [![NoSQL](https://img.shields.io/badge/db-NoSQL-pink.svg?tyle=flat)](https://github.com/jacoblai/dhdb)

## 功能支持

* 纯Go编写
* 10亿级数据量支持
* 集成先进先出数据队列
* 支持KV集合 (Z-LIST有序集合，主键不允许重复)
* 支持 TTL 超时自动删除及通知事件
* 支持嵌入式设备OPENWRT等系统 (ARM/MIPS)

## 使用说明

* 中文手册参阅[项目WIKI!](https://github.com/jacoblai/yiyidb/wiki)
* 更多示例代码参阅项目 *_test.go 文件

## 快速上手
```
import "github.com/jacoblai/yiyidb"
```
## 重打开或创建一个数据库
```
//取到当前程序所在物理位置
dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
if err != nil {
	panic(err)
}
//创建或重打开数据库
//参数1 数据库路径,
//参数2 是否开启同库数据分组(chan库开启标识) 
//参数3 是否开启ttl自动删除记录,当开启ttl后put操作的ttl才会生效
//参数4 数据碰测优化，输入可能出现key的最大长度
kv, err := yiyidb.OpenKvdb(dir, false, false, 10)
if err != nil {
	panic(err)
}
defer kv.Close()
```
## 注册当TTL超时删除事件通知
```
kv.OnExpirse = func(key, value []byte) {
   fmt.Println("exp:", string(key), string(value))
}
```

## 插入一条记录，(当重复Put同key时操作等同于更新内容操作)
```
kv.Put([]byte("hello1"), []byte("hello value"), 0)//key值，value值， 0为永不超时
```
## 设置一条已存在记录并8秒后超时自动删除
```
kv.SetTTL([]byte("hello1"), 8)
```

## 删除一条记录
```
kv.Del([]byte("hello1"))
```

## 性能 
## 插入队列压力测试
300,000	      5865ns/op	     516B/op	       9allocs/op
## 取出队列压力测试
200,000	     14379ns/op	    1119B/op	      20allocs/op

## 作者
@jacoblai

## 感谢

* syndtr, github.com/syndtr/goleveldb
