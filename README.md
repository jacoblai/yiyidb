# YIYIDB - A fast NoSQL database for storing big list of data

[![Author](https://img.shields.io/badge/author-@jacoblai-blue.svg?style=flat)](http://www.icoolpy.com/) [![Platform](https://img.shields.io/badge/platform-Linux,%20OpenWrt,%20Android,%20Mac,%20Windows-green.svg?style=flat)](https://github.com/jacoblai/dhdb) [![NoSQL](https://img.shields.io/badge/db-NoSQL-pink.svg?tyle=flat)](https://github.com/jacoblai/dhdb)


YIYIDB 高性能的no-sql数据库

## 功能支持

* 纯golang编写
* 10亿级数据量支持
* 集成先进先出数据队列
* 支持KV集合 (z-list有序集合，主键不允许重复)
* 支持 TTL 超时自动删除及通知事件
* 支持嵌入式设备OpenWrt等系统 (ARM/MIPS)

## 更新示例代码在项目中的 *_test.go 文件中

## 性能 
## 插入队列压力测试
300,000	      5865ns/op	     516B/op	       9allocs/op
## 取出队列压力测试
200,000	     14379ns/op	    1119B/op	      20allocs/op

## 作者
@jacoblai

## 感谢

* syndtr, github.com/syndtr/goleveldb
