package yiyidb

import (
	"sync"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/util"
	"bytes"
	"errors"
	"os"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Kvdb struct {
	sync.RWMutex
	DataDir      string
	db           *leveldb.DB
	ttldb        *ttlRunner
	iteratorOpts *opt.ReadOptions
	syncOpts     *opt.WriteOptions
	OnExpirse    func(key, value []byte)
}

func OpenKvdb(dataDir string) (*Kvdb, error) {
	var err error

	// Create a new Queue.
	kv := &Kvdb{
		DataDir:      dataDir,
		db:           &leveldb.DB{},
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

	kv.syncOpts = &opt.WriteOptions{}
	kv.syncOpts.Sync = true

	// Open database for the queue.
	kv.db, err = leveldb.OpenFile(kv.DataDir, opts)
	if err != nil {
		return nil, err
	}
	//Open TTl
	kv.ttldb, err = OpenTtlRunner(kv.db, kv.DataDir)
	if err != nil {
		return nil, err
	}
	kv.ttldb.HandleExpirse = kv.onExp
	//run ttl func
	kv.ttldb.Run()
	return kv, nil
}

func (k *Kvdb) IterAll() map[string]*[]byte {
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	res := make(map[string]*[]byte)
	for iter.Next(){
		res[string(iter.Key())] = &iter.Value()
	}
	return res
}

func (k *Kvdb) Drop() {
	k.Close()
	os.RemoveAll(k.DataDir)
}

func (k *Kvdb) onExp(key, value []byte) {
	if k.OnExpirse != nil {
		k.OnExpirse(key, value)
	}
}

func (k *Kvdb) NilTTL(key []byte) error {
	if k.ttldb.Exists(key) {
		return k.ttldb.SetTTL(-1, key)
	} else {
		return errors.New("ttl not found")
	}
}

func (k *Kvdb) SetTTL(key []byte, ttl int) error {
	if k.Exists(key) {
		if ttl > 0 {
			return k.ttldb.SetTTL(ttl, key)
		} else {
			return errors.New("must > 0")
		}
	} else {
		return errors.New("records not found")
	}
}

func (k *Kvdb) GetTTL(key []byte) (float64, error) {
	return k.ttldb.GetTTL(key)
}

func (k *Kvdb) Exists(key []byte) bool {
	ok, _ := k.db.Has(key, k.iteratorOpts)
	return ok
}

func (k *Kvdb) Get(key []byte) ([]byte, error) {
	data, err := k.db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (k *Kvdb) GetObject(key []byte, value interface{}) error {
	data, err := k.Get(key)
	if err != nil {
		return err
	}
	err = msgpack.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	return nil
}

func (k *Kvdb) Put(key, value []byte, ttl int) error {
	err := k.db.Put(key, value, nil)
	if err != nil {
		return err
	}
	if ttl > 0 {
		k.ttldb.SetTTL(ttl, key)
	}
	return nil
}

func (k *Kvdb) PutObject(key []byte, value interface{}, ttl int) error {
	msg, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	return k.Put(key, msg, ttl)
}

func (k *Kvdb) BatPutOrDel(items *[]BatItem) error {
	batch := new(leveldb.Batch)
	for _, v := range *items {
		switch v.Op {
		case "put":
			batch.Put(v.Key, v.Value)
			if v.Ttl > 0 {
				k.ttldb.SetTTL(v.Ttl, v.Key)
			}
		case "del":
			batch.Delete(v.Key)
			k.ttldb.DelTTL(v.Key)
		}
	}
	return k.db.Write(batch, nil)
}

func (k *Kvdb) Del(key []byte) error {
	err := k.db.Delete(key, k.syncOpts)
	if err != nil {
		return err
	}
	k.ttldb.DelTTL(key)
	return nil
}

// allKeys returns all keys. Sorted.
func (k *Kvdb) AllKeys() []string {
	var keys []string
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Release()
	//sort.Strings(keys) // To make things deterministic.
	return keys
}

func (k *Kvdb) KeyStart(key []byte) []string {
	var keys []string
	iter := k.db.NewIterator(util.BytesPrefix(key), k.iteratorOpts)
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Release()
	return keys
}

func (k *Kvdb) KeyRange(min, max []byte) []string {
	var keys []string
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	for ok := iter.Seek(min); ok && bytes.Compare(iter.Key(), max) <= 0; ok = iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Release()
	return keys
}

func (k *Kvdb) Close() error {
	err := k.db.Close()
	if err != nil {
		return err
	}
	return nil
}
