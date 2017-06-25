package yiyidb

import (
	"sync"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/util"
	"bytes"
)

type Kvdb struct {
	sync.RWMutex
	DataDir      string
	db           *leveldb.DB
	ttldb        *ttlRunner
	iteratorOpts *opt.ReadOptions
	syncOpts     *opt.WriteOptions
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

	kv.ttldb.startCleanupTimer()
	return kv, nil
}

func (k *Kvdb) Exists(key string) bool {
	ok, _ := k.db.Has([]byte(key), nil)
	return ok
}

func (k *Kvdb) Get(key []byte) ([]byte, error) {
	data, err := k.db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (k *Kvdb) Put(key,value []byte, ttl int) error {
	err := k.db.Put(key, value, nil)
	if err != nil {
		return err
	}
	if ttl != 0{
		k.ttldb.Put(ttl, key)
	}
	return nil
}

// allKeys returns all keys. Sorted.
func (k *Kvdb) AllKeys() []string {
	var keys []string
	iter := k.db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	//sort.Strings(keys) // To make things deterministic.
	return keys
}

func (k *Kvdb) KeyStart(key string) []string {
	var keys []string
	iter := k.db.NewIterator(util.BytesPrefix([]byte(key)), nil)
	defer iter.Release()
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	return keys
}

func (k *Kvdb) KeyRange(min string, max string) []string {
	var keys []string
	iter := k.db.NewIterator(nil, nil)
	defer iter.Release()
	for ok := iter.Seek([]byte(min)); ok && bytes.Compare(iter.Key(), []byte(max)) <= 0; ok = iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	return keys
}

func (k *Kvdb) Close() error {
	err := k.db.Close()
	if err != nil {
		return err
	}
	return nil
}