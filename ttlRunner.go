package yiyidb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"time"
	"gopkg.in/vmihailenco/msgpack.v2"
	"fmt"
)

type ttlRunner struct {
	masterdb      *leveldb.DB
	db            *leveldb.DB
	iteratorOpts  *opt.ReadOptions
	quit          chan struct{}
	HandleExpirse func(key, value []byte)
	IsWorking     bool
}

func OpenTtlRunner(masterdb *leveldb.DB, dbname string, defaultBloomBits int) (*ttlRunner, error) {
	var err error
	ttl := &ttlRunner{
		masterdb:     masterdb,
		iteratorOpts: &opt.ReadOptions{DontFillCache: true},
		quit:         make(chan struct{}, 1),
		IsWorking:    false,
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

func (t *ttlRunner) Exists(key []byte) bool {
	ok, _ := t.db.Has(key, t.iteratorOpts)
	return ok
}

func (t *ttlRunner) SetTTL(expires int, masterDbKey []byte) error {
	//设置大于0值即设置ttl以秒为单位
	if expires > 0 {
		ttl := &TtlItem{
			Dkey: masterDbKey,
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

func (t *ttlRunner) GetTTL(key []byte) (float64, error) {
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

func (t *ttlRunner) DelTTL(key []byte) error {
	return t.db.Delete(key, nil)
}

func (t *ttlRunner) Run() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				if !t.IsWorking {
					t.IsWorking = true
					batch := new(leveldb.Batch)
					iter := t.db.NewIterator(nil, t.iteratorOpts)
					for iter.Next() {
						var it TtlItem
						if err := msgpack.Unmarshal(iter.Value(), &it); err != nil {
							t.db.Delete(iter.Key(), nil)
						} else {
							if it.expired() {
								batch.Delete(it.Dkey)
								val, err := t.masterdb.Get(iter.Key(), t.iteratorOpts)
								if err == nil && t.HandleExpirse != nil {
									t.HandleExpirse(iter.Key(), val)
								}
							}
						}
					}
					iter.Release()
					if batch.Len() > 0 {
						if err := t.masterdb.Write(batch, nil); err != nil {
							fmt.Println(err)
						}
						if err := t.db.Write(batch, nil); err != nil {
							fmt.Println(err)
						}
					}
					t.IsWorking = false
				}
			case <-t.quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (t *ttlRunner) Close() {
	close(t.quit)
}
