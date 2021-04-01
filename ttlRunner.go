package yiyidb

/*
#include "ticker.h"
*/
import "C"
import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"sync"
	"time"
)

type TtlRunner struct {
	masterdb      *leveldb.DB
	db            *leveldb.DB
	iteratorOpts  *opt.ReadOptions
	HandleExpirse func(key, value []byte)
	IsWorking     bool
	batch         *leveldb.Batch
}

var ttlrn *TtlRunner

//export Gotask
func Gotask() {
	if !ttlrn.IsWorking {
		ttlrn.IsWorking = true
		ttlrn.batch.Reset()
		iter := ttlrn.db.NewIterator(nil, ttlrn.iteratorOpts)
		for iter.Next() {
			var it TtlItem
			if err := msgpack.Unmarshal(iter.Value(), &it); err != nil {
				ttlrn.db.Delete(iter.Key(), nil)
			} else {
				if it.expired() {
					ttlrn.batch.Delete(it.Dukey)
					val, err := ttlrn.masterdb.Get(iter.Key(), ttlrn.iteratorOpts)
					if err == nil && ttlrn.HandleExpirse != nil {
						ttlrn.HandleExpirse(iter.Key(), val)
					}
				}
			}
		}
		iter.Release()
		if ttlrn.batch.Len() > 0 {
			if err := ttlrn.masterdb.Write(ttlrn.batch, nil); err != nil {
				fmt.Println(err)
			}
			if err := ttlrn.db.Write(ttlrn.batch, nil); err != nil {
				fmt.Println(err)
			}
		}
		ttlrn.IsWorking = false
	}
}

func OpenTtlRunner(masterdb *leveldb.DB, dbname string, defaultBloomBits int) (*TtlRunner, error) {
	var err error
	ttl := &TtlRunner{
		masterdb:     masterdb,
		iteratorOpts: &opt.ReadOptions{DontFillCache: true},
		IsWorking:    false,
		batch:        &leveldb.Batch{},
	}
	opts := &opt.Options{}
	opts.ErrorIfMissing = false
	opts.BlockCacheCapacity = 4 * MB
	opts.Filter = filter.NewBloomFilter(defaultBloomBits)
	opts.Compression = opt.SnappyCompression
	opts.BlockSize = 4 * KB
	opts.WriteBuffer = 4 * MB
	opts.OpenFilesCacheCapacity = 1 * KB
	opts.CompactionTableSize = 32 * MB
	opts.WriteL0SlowdownTrigger = 16
	opts.WriteL0PauseTrigger = 64

	//Open TTl
	ttl.db, err = leveldb.OpenFile(dbname+"/ttl", opts)
	if err != nil {
		return nil, err
	}
	return ttl, nil
}

func (t *TtlRunner) Exists(key []byte) bool {
	ok, _ := t.db.Has(key, t.iteratorOpts)
	return ok
}

func (t *TtlRunner) SetTTL(expires int, masterDbKey []byte) error {
	//设置大于0值即设置ttl以秒为单位
	if expires > 0 {
		ttl := &TtlItem{
			Dukey: masterDbKey,
		}
		ttl.touch(time.Duration(expires) * time.Second)
		ttlitem, _ := msgpack.Marshal(ttl)
		if err := t.db.Put(masterDbKey, ttlitem, nil); err != nil {
			return err
		}
	} else if expires < 0 {
		//设置少于0值即取消此记当的TTL属性
		if err := t.db.Delete(masterDbKey, nil); err != nil {
			return err
		}
	}
	return nil
}

func (t *TtlRunner) GetTTL(key []byte) (float64, error) {
	val, err := t.db.Get(key, t.iteratorOpts)
	if err != nil {
		return 0, err
	}
	var it TtlItem
	if err := msgpack.Unmarshal(val, &it); err != nil {
		return 0, err
	}
	return it.Expires.Sub(time.Now()).Seconds(), nil
}

func (t *TtlRunner) DelTTL(key []byte) error {
	return t.db.Delete(key, nil)
}

func (t *TtlRunner) Run() {
	ttlrn = t
	C.cticker()
}

func (t *TtlRunner) Close() {
	_ = t.db.Close()
}

type TtlItem struct {
	sync.RWMutex
	Dukey   []byte
	Expires *time.Time
}

func (item *TtlItem) touch(duration time.Duration) {
	item.Lock()
	expiration := time.Now().Add(duration)
	item.Expires = &expiration
	item.Unlock()
}

func (item *TtlItem) expired() bool {
	value := false
	item.RLock()
	if item.Expires == nil {
		value = true
	} else {
		value = time.Now().After(*item.Expires)
	}
	item.RUnlock()
	return value
}
