package yiyidb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"log"
	"time"
)

type TtlRunner struct {
	masterdb      *leveldb.DB
	db            *leveldb.DB
	iteratorOpts  *opt.ReadOptions
	quit          chan struct{}
	HandleExpirse func(key, value []byte)
}

func OpenTtlRunner(masterdb *leveldb.DB, dbname string, defaultBloomBits int) (*TtlRunner, error) {
	var err error
	ttl := &TtlRunner{
		masterdb:     masterdb,
		iteratorOpts: &opt.ReadOptions{DontFillCache: true},
		quit:         make(chan struct{}, 1),
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
		exp := time.Now().Add(time.Duration(expires) * time.Second)
		if err := t.db.Put(masterDbKey, IdToKeyPure(exp.UnixNano()), nil); err != nil {
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
	exp := time.Unix(0, KeyToIDPure(val))
	return exp.Sub(time.Now()).Seconds(), nil
}

func (t *TtlRunner) DelTTL(key []byte) error {
	return t.db.Delete(key, nil)
}

func (t *TtlRunner) Run() {
	for {
		select {
		case <-t.quit:
			return
		default:
			ct := 0
			batch := new(leveldb.Batch)
			iter := t.db.NewIterator(nil, t.iteratorOpts)
			for iter.Next() {
				exp := time.Unix(0, KeyToIDPure(iter.Value()))
				if time.Now().After(exp) {
					ct++
					if ct >= 100000 {
						break
					}
					k := iter.Key()
					batch.Delete(k)
					val, err := t.masterdb.Get(k, nil)
					if err == nil && t.HandleExpirse != nil {
						t.HandleExpirse(k, val)
					}
				}
			}
			iter.Release()
			if batch.Len() > 0 {
				if err := t.masterdb.Write(batch, nil); err != nil {
					log.Println(err)
				}
				if err := t.db.Write(batch, nil); err != nil {
					log.Println(err)
				}
				time.Sleep(5 * time.Millisecond)
			} else {
				time.Sleep(5 * time.Second)
			}
		}
	}
}

func (t *TtlRunner) Close() {
	close(t.quit)
}
