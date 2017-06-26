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
	masterdb     *leveldb.DB
	db           *leveldb.DB
	iteratorOpts *opt.ReadOptions
}

func OpenTtlRunner(masterdb *leveldb.DB, dbname string) (*ttlRunner, error) {
	var err error
	ttl := &ttlRunner{
		masterdb:     masterdb,
		iteratorOpts: &opt.ReadOptions{DontFillCache: true},
	}
	opts := &opt.Options{}
	opts.ErrorIfMissing = false
	opts.BlockCacheCapacity = 4 * MB
	opts.Filter = filter.NewBloomFilter(defaultFilterBits)
	opts.Compression = opt.SnappyCompression
	opts.BlockSize = 4 * KB
	opts.WriteBuffer = 4 * MB
	opts.OpenFilesCacheCapacity = 1 * KB
	opts.CompactionTableSize = 32 * MB
	opts.WriteL0SlowdownTrigger = 16
	opts.WriteL0PauseTrigger = 64

	//Open TTl
	ttl.db, err = leveldb.OpenFile(dbname+"_ttl", opts)
	if err != nil {
		return nil, err
	}

	return ttl, nil
}

func (t *ttlRunner) Put(expires int, masterDbKey []byte) error {
	if expires > 0 {
		ttl := &ItemTtl{
			Dkey: masterDbKey,
		}
		ttl.touch(time.Duration(expires) * time.Second)
		ttlitem, _ := msgpack.Marshal(ttl)
		if err := t.db.Put(masterDbKey, ttlitem, nil); err != nil {
			return err
		}
	} else if expires < 0 {
		//当设置为0或更小数时删除ttl记录
		if err := t.db.Delete(masterDbKey, nil); err != nil {
			return err
		}
	}
	return nil
}

func (t *ttlRunner) Run() {
	isTtl := false
	go func() {
		for {
			if !isTtl {
				m := time.Now().Add(1* time.Second)
				isTtl = true
				batch := new(leveldb.Batch)
				iter := t.db.NewIterator(nil, t.iteratorOpts)
				for iter.Next() {
					var it ItemTtl
					if err := msgpack.Unmarshal(iter.Value(), &it); err != nil {
						t.db.Delete(iter.Key(), nil)
					} else {
						if it.expired() {
							batch.Delete(it.Dkey)
							fmt.Println("expirse", it.Expires)
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
				exp := m.Sub(time.Now())
				if exp.Seconds() > 0 {
					time.Sleep(exp)
				}
				isTtl = false
			}
		}
	}()
}
