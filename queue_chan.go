package yiyidb

import (
	"sync"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"errors"
	"github.com/syndtr/goleveldb/leveldb/util"
	"bytes"
	"encoding/binary"
	"os"
)

//FIFO
type QueueChan struct {
	sync.RWMutex
	DataDir      string
	db           *leveldb.DB
	head         uint64
	tail         uint64
	isOpen       bool
	iteratorOpts *opt.ReadOptions
	maxkv        int
}

func OpenQueueChan(dataDir string, defaultKeyLen int) (*QueueChan, error) {
	var err error

	q := &QueueChan{
		DataDir:      dataDir,
		db:           &leveldb.DB{},
		head:         0,
		tail:         0,
		isOpen:       false,
		iteratorOpts: &opt.ReadOptions{DontFillCache: true},
		maxkv:        512 * MB,
	}

	opts := &opt.Options{}
	opts.ErrorIfMissing = false
	opts.BlockCacheCapacity = 4 * MB
	//队列key固定用8个byte所以bloom应该是8*1.44~12优化查询+channel name len
	opts.Filter = filter.NewBloomFilter(int(12) + defaultKeyLen)
	opts.Compression = opt.SnappyCompression
	opts.BlockSize = 4 * KB
	opts.WriteBuffer = 4 * MB
	opts.OpenFilesCacheCapacity = 1 * KB
	opts.CompactionTableSize = 32 * MB
	opts.WriteL0SlowdownTrigger = 16
	opts.WriteL0PauseTrigger = 64

	q.db, err = leveldb.OpenFile(dataDir, opts)
	if err != nil {
		return nil, err
	}
	q.isOpen = true
	return q, nil
}

func (q *QueueChan) Enqueue(chname string, value []byte) (*QueueItem, error) {
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if len(value) > q.maxkv {
		return nil, errors.New("out of len 512M")
	}
	fix := chname + "-"
	iter := q.db.NewIterator(util.BytesPrefix([]byte(fix)), q.iteratorOpts)
	defer iter.Release()
	iter.Last()
	tail := q.keyToID(iter.Key()) + 1
	item := &QueueItem{
		ID:    tail,
		Key:   q.idToKey(fix,tail),
		Value: value,
	}
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}
	return item, nil
}

func (q *QueueChan) Dequeue(chname string) (*QueueItem, error) {
	q.RLock()
	defer q.RUnlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	fix := chname + "-"
	iter := q.db.NewIterator(util.BytesPrefix([]byte(fix)), q.iteratorOpts)
	defer iter.Release()
	iter.First()

	item := &QueueItem{ID: q.keyToID(iter.Key()), Key: iter.Key()}
	item.Value, _ = q.db.Get(item.Key, nil)
	if err := q.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}
	return item, nil
}

func (q *QueueChan) idToKey(chname string, id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	rfix := make([]byte,8+len(chname))
	fix := []byte(chname)
	copy(rfix,fix)
	copy(rfix[len(fix):],key[:])
	return rfix
}

func (q *QueueChan) keyToID(key []byte) uint64 {
	k := bytes.Split(key, []byte("-"))
	if len(k) == 2 {
		return binary.BigEndian.Uint64(k[1])
	}
	return 0
}

func (q *QueueChan) Length(chname string) uint64 {
	fix := chname + "-"
	iter := q.db.NewIterator(util.BytesPrefix([]byte(fix)), q.iteratorOpts)
	defer iter.Release()
	iter.First()
	tail := q.keyToID(iter.Key())
	iter.Last()
	head := q.keyToID(iter.Key())
	return tail - head
}

func (q *QueueChan) Drop() {
	q.Close()
	os.RemoveAll(q.DataDir)
}

func (q *QueueChan) Close() {
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return
	}
	q.db.Close()
	q.isOpen = false
}