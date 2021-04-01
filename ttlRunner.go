package yiyidb

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"log"
	"sync"
	"time"
)

type TtlRunner struct {
	masterdb      *leveldb.DB
	db            *leveldb.DB
	iteratorOpts  *opt.ReadOptions
	quit          chan struct{}
	HandleExpirse func(key, value []byte)
	IsWorking     bool
	batch         *leveldb.Batch
}

func OpenTtlRunner(masterdb *leveldb.DB, dbname string, defaultBloomBits int) (*TtlRunner, error) {
	var err error
	ttl := &TtlRunner{
		masterdb:     masterdb,
		iteratorOpts: &opt.ReadOptions{DontFillCache: true},
		quit:         make(chan struct{}, 1),
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
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				if !t.IsWorking {
					ct := 0
					t.IsWorking = true
					t.batch.Reset()
					iter := t.db.NewIterator(nil, t.iteratorOpts)
					for iter.Next() {
						ct++
						var it TtlItem
						if err := msgpack.Unmarshal(iter.Value(), &it); err != nil {
							t.db.Delete(iter.Key(), nil)
						} else {
							if it.expired() {
								t.batch.Delete(it.Dukey)
								val, err := t.masterdb.Get(iter.Key(), t.iteratorOpts)
								if err == nil && t.HandleExpirse != nil {
									t.HandleExpirse(iter.Key(), val)
								}
							}
						}
					}
					iter.Release()
					if t.batch.Len() > 0 {
						if err := t.masterdb.Write(t.batch, nil); err != nil {
							fmt.Println(err)
						}
						if err := t.db.Write(t.batch, nil); err != nil {
							fmt.Println(err)
						}
					}
					log.Println("has", ct, "bat", t.batch.Len())
					t.IsWorking = false
				}
			case <-t.quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (t *TtlRunner) Close() {
	_ = t.db.Close()
	close(t.quit)
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
