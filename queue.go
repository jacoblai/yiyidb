package yiyidb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"os"
	"sync/atomic"
)

//FIFO
type Queue struct {
	DataDir      string
	db           *leveldb.DB
	head         int64
	tail         int64
	isOpen       bool
	iteratorOpts *opt.ReadOptions
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
	if !q.isOpen {
		return ErrDBClosed
	}

	batch := new(leveldb.Batch)
	for _, v := range value {
		atomic.AddInt64(&q.tail, 1)
		tail := atomic.LoadInt64(&q.tail)
		item := &QueueItem{
			ID:    tail,
			Key:   IdToKeyPure(tail),
			Value: v,
		}
		batch.Put(item.Key, item.Value)
	}
	if err := q.db.Write(batch, nil); err != nil {
		atomic.AddInt64(&q.tail, -int64(len(value)))
		return err
	}

	return nil
}

func (q *Queue) Enqueue(value []byte) (*QueueItem, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	atomic.AddInt64(&q.tail, 1)
	tail := atomic.LoadInt64(&q.tail)
	item := &QueueItem{
		ID:    tail,
		Key:   IdToKeyPure(tail),
		Value: value,
	}
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		atomic.AddInt64(&q.tail, -1)
		return nil, err
	}
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
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	head := atomic.LoadInt64(&q.head)
	item, err := q.getItemByID(head + 1)
	if err != nil {
		return nil, err
	}
	if err = q.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}
	atomic.AddInt64(&q.head, 1)
	head = atomic.LoadInt64(&q.head)
	tail := atomic.LoadInt64(&q.tail)
	//当队列取空后重置游标
	if head == tail {
		atomic.StoreInt64(&q.head, 0)
		atomic.StoreInt64(&q.tail, 0)
	}
	return item, nil
}

func (q *Queue) Peek() (*QueueItem, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	head := atomic.LoadInt64(&q.head)
	return q.getItemByID(head + 1)
}

func (q *Queue) PeekByOffset(offset int64) (*QueueItem, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	head := atomic.LoadInt64(&q.head)
	return q.getItemByID(head + offset + 1)
}

func (q *Queue) PeekByID(id int64) (*QueueItem, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	return q.getItemByID(id)
}

func (q *Queue) Update(id int64, newValue []byte) (*QueueItem, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	head := atomic.LoadInt64(&q.head)
	tail := atomic.LoadInt64(&q.tail)
	if id <= head || id > tail {
		return nil, ErrOutOfBounds
	}
	item := &QueueItem{
		ID:    id,
		Key:   IdToKeyPure(id),
		Value: newValue,
	}
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}

	return item, nil
}

func (q *Queue) UpdateString(id int64, newValue string) (*QueueItem, error) {
	return q.Update(id, []byte(newValue))
}

func (q *Queue) UpdateObject(id int64, newValue interface{}) (*QueueItem, error) {
	msg, err := msgpack.Marshal(newValue)
	if err != nil {
		return nil, err
	}
	return q.Update(id, msg)
}

func (q *Queue) Length() int64 {
	head := atomic.LoadInt64(&q.head)
	tail := atomic.LoadInt64(&q.tail)
	return tail - head
}

func (q *Queue) Close() {
	if !q.isOpen {
		return
	}
	q.isOpen = false
	_ = q.db.Close()
}

func (q *Queue) Drop() {
	q.Close()
	_ = os.RemoveAll(q.DataDir)
}

func (q *Queue) getItemByID(id int64) (*QueueItem, error) {
	tail := atomic.LoadInt64(&q.tail)
	head := atomic.LoadInt64(&q.head)
	if tail-head == 0 {
		return nil, ErrEmpty
	} else if id <= head || id > tail {
		return nil, ErrOutOfBounds
	}
	var err error
	item := &QueueItem{ID: id, Key: IdToKeyPure(id)}
	if item.Value, err = q.db.Get(item.Key, nil); err != nil {
		return nil, err
	}
	return item, nil
}

func (q *Queue) init() error {
	iter := q.db.NewIterator(nil, q.iteratorOpts)
	defer iter.Release()
	if ok := iter.First(); ok {
		atomic.StoreInt64(&q.head, KeyToIDPure(iter.Key())-1)
	} else {
		atomic.StoreInt64(&q.head, 0)
	}
	if ok := iter.Last(); ok {
		atomic.StoreInt64(&q.tail, KeyToIDPure(iter.Key()))
	} else {
		atomic.StoreInt64(&q.tail, 0)
	}
	return iter.Error()
}
