package yiyidb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"gopkg.in/vmihailenco/msgpack.v2"
	"sync"
	"os"
	"errors"
)

//FIFO
type Queue struct {
	sync.RWMutex
	DataDir      string
	db           *leveldb.DB
	head         uint64
	tail         uint64
	isOpen       bool
	iteratorOpts *opt.ReadOptions
	maxkv        int
}

func OpenQueue(dataDir string) (*Queue, error) {
	var err error

	q := &Queue{
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
	//队列key固定用8个byte所以bloom应该是8*1.44~12优化查询
	opts.Filter = filter.NewBloomFilter(int(12))
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
	return q, q.init()
}

func (q *Queue) EnqueueBatch(value [][]byte) error {
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return ErrDBClosed
	}

	batch := new(leveldb.Batch)
	for _, v := range value {
		if len(v) > q.maxkv {
			return errors.New("out of len 512M")
		}
		item := &QueueItem{
			ID:    q.tail + 1,
			Key:   idToKeyPure(q.tail + 1),
			Value: v,
		}
		batch.Put(item.Key, item.Value)
		q.tail++
	}
	if err := q.db.Write(batch, nil); err != nil {
		return err
	}

	return nil
}

func (q *Queue) Enqueue(value []byte) (*QueueItem, error) {
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if len(value) > q.maxkv {
		return nil, errors.New("out of len 512M")
	}
	item := &QueueItem{
		ID:    q.tail + 1,
		Key:   idToKeyPure(q.tail + 1),
		Value: value,
	}
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}
	q.tail++
	return item, nil
}

func (q *Queue) EnqueueString(value string) (*QueueItem, error) {
	return q.Enqueue([]byte(value))
}

func (q *Queue) EnqueueObject(value interface{}) (*QueueItem, error) {
	msg, err := msgpack.Marshal(value)
	if err != nil {
		return nil, err
	}
	return q.Enqueue(msg)
}

func (q *Queue) Dequeue() (*QueueItem, error) {
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	item, err := q.getItemByID(q.head + 1)
	if err != nil {
		return nil, err
	}
	if err := q.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}
	q.head++
	//当队列取空后重置游标
	if q.head == q.tail {
		q.head = 0
		q.tail = 0
	}
	return item, nil
}

func (q *Queue) Peek() (*QueueItem, error) {
	q.RLock()
	defer q.RUnlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	return q.getItemByID(q.head + 1)
}

func (q *Queue) PeekByOffset(offset uint64) (*QueueItem, error) {
	q.RLock()
	defer q.RUnlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	return q.getItemByID(q.head + offset + 1)
}

func (q *Queue) PeekByID(id uint64) (*QueueItem, error) {
	q.RLock()
	defer q.RUnlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	return q.getItemByID(id)
}

func (q *Queue) Update(id uint64, newValue []byte) (*QueueItem, error) {
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if id <= q.head || id > q.tail {
		return nil, ErrOutOfBounds
	}
	item := &QueueItem{
		ID:    id,
		Key:   idToKeyPure(id),
		Value: newValue,
	}
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}

	return item, nil
}

func (q *Queue) UpdateString(id uint64, newValue string) (*QueueItem, error) {
	return q.Update(id, []byte(newValue))
}

func (q *Queue) UpdateObject(id uint64, newValue interface{}) (*QueueItem, error) {
	msg, err := msgpack.Marshal(newValue)
	if err != nil {
		return nil, err
	}
	return q.Update(id, msg)
}

func (q *Queue) Length() uint64 {
	return q.tail - q.head
}

func (q *Queue) Close() {
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return
	}
	q.head = 0
	q.tail = 0
	q.db.Close()
	q.isOpen = false
}

func (q *Queue) Drop() {
	q.Close()
	os.RemoveAll(q.DataDir)
}

func (q *Queue) getItemByID(id uint64) (*QueueItem, error) {
	if q.Length() == 0 {
		return nil, ErrEmpty
	} else if id <= q.head || id > q.tail {
		return nil, ErrOutOfBounds
	}
	var err error
	item := &QueueItem{ID: id, Key: idToKeyPure(id)}
	if item.Value, err = q.db.Get(item.Key, nil); err != nil {
		return nil, err
	}
	return item, nil
}

func (q *Queue) init() error {
	iter := q.db.NewIterator(nil, q.iteratorOpts)
	defer iter.Release()
	if iter.First() {
		q.head = keyToIDPure(iter.Key()) - 1
	}
	if iter.Last() {
		q.tail = keyToIDPure(iter.Key())
	}
	return iter.Error()
}
