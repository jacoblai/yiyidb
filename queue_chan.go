package yiyidb

import (
	"bytes"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/vmihailenco/msgpack.v2"
	"os"
	"sync"
	"sync/atomic"
)

//FIFO
type ChanQueue struct {
	DataDir      string
	db           *leveldb.DB
	isOpen       bool
	iteratorOpts *opt.ReadOptions
	mats         *sync.Map //map[string]*mat
}

type Mat struct {
	MixName string
	Head    int64
	Tail    int64
}

func OpenChanQueue(dataDir string, defaultKeyLen int) (*ChanQueue, error) {
	var err error
	q := &ChanQueue{
		DataDir:      dataDir,
		db:           &leveldb.DB{},
		isOpen:       false,
		mats:         &sync.Map{},
		iteratorOpts: &opt.ReadOptions{DontFillCache: true},
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
	return q, q.init()
}

func (q *ChanQueue) init() error {
	iter := q.db.NewIterator(nil, q.iteratorOpts)
	defer iter.Release()
	for iter.Next() {
		mixName := keyName(iter.Key())
		if _, ok := q.mats.Load(mixName); !ok {
			q.mats.Store(mixName, &Mat{MixName: mixName, Head: -1, Tail: 0})
		}
	}
	q.mats.Range(func(k, v interface{}) bool {
		ma := v.(*Mat)
		rg := util.BytesPrefix(k.([]byte))
		iter.Seek(rg.Start)
		atomic.StoreInt64(&ma.Head, keyToID(iter.Key())-1)
		var lastid int64
		for ok := iter.Seek(rg.Start); ok && bytes.Compare(iter.Key(), rg.Limit) <= 0; ok = iter.Next() {
			lastid = keyToID(iter.Key())
		}
		atomic.StoreInt64(&ma.Tail, lastid)
		return true
	})
	return iter.Error()
}

func (q *ChanQueue) EnqueueObject(chname string, value interface{}) (*QueueItem, error) {
	msg, err := msgpack.Marshal(value)
	if err != nil {
		return nil, err
	}
	return q.Enqueue(chname, msg)
}

func (q *ChanQueue) Enqueue(chname string, value []byte) (*QueueItem, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if mt, ok := q.mats.Load(chname); ok {
		ma := mt.(*Mat)
		atomic.AddInt64(&ma.Tail, 1)
		if err := q.db.Put(idToKey(ma.MixName, ma.Tail), value, nil); err != nil {
			atomic.AddInt64(&ma.Tail, -1)
			return nil, err
		}
		return &QueueItem{ID: ma.Tail, Key: idToKey(chname, ma.Tail), Value: value}, nil
	} else {
		item := &QueueItem{ID: 1, Key: idToKey(chname, 1), Value: value}
		if err := q.db.Put(item.Key, item.Value, nil); err != nil {
			return nil, err
		}
		q.mats.Store(chname, &Mat{MixName: chname, Tail: item.ID, Head: 0})
		return item, nil
	}
}

func (q *ChanQueue) Dequeue(chname string) (*QueueItem, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if mt, ok := q.mats.Load(chname); ok {
		ma := mt.(*Mat)
		atomic.AddInt64(&ma.Head, 1)
		item, err := q.getItemByID(chname, ma.Head)
		if err != nil {
			return nil, err
		}
		if err := q.db.Delete(item.Key, nil); err != nil {
			atomic.AddInt64(&ma.Head, -1)
			return nil, err
		}
		//当队列取空后重置游标
		if ma.Head == ma.Tail {
			atomic.StoreInt64(&ma.Head, 0)
			atomic.StoreInt64(&ma.Tail, 0)
		}
		return item, nil
	} else {
		return nil, errors.New("ch not ext")
	}
}

func (q *ChanQueue) GetMetal(chname string) (int64, int64) {
	if !q.isOpen {
		return 0, 0
	}
	if mt, ok := q.mats.Load(chname); ok {
		ma := mt.(*Mat)
		return ma.Head, ma.Tail
	} else {
		return 0, 0
	}
}

func (q *ChanQueue) GetChans() ([]string, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	chans := make([]string, 0)
	q.mats.Range(func(k, _ interface{}) bool {
		chans = append(chans, k.(string))
		return true
	})
	return chans, nil
}

func (q *ChanQueue) Length(chname string) (int64, error) {
	if !q.isOpen {
		return 0, ErrDBClosed
	}
	if mt, ok := q.mats.Load(chname); ok {
		ma := mt.(*Mat)
		return ma.Tail - ma.Head, nil
	} else {
		return 0, errors.New("ch not ext")
	}
}

func (q *ChanQueue) Clear(chname string) error {
	if !q.isOpen {
		return ErrDBClosed
	}
	if mt, ok := q.mats.Load(chname); ok {
		ma := mt.(*Mat)
		atomic.StoreInt64(&ma.Head, 0)
		atomic.StoreInt64(&ma.Tail, 0)
	} else {
		return errors.New("ch not ext")
	}
	batch := new(leveldb.Batch)
	iter := q.db.NewIterator(util.BytesPrefix(append([]byte(chname), 0xFF)), q.iteratorOpts)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := q.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

func (q *ChanQueue) Peek(chname string) (*QueueItem, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if mt, ok := q.mats.Load(chname); ok {
		ma := mt.(*Mat)
		item, err := q.getItemByID(chname, ma.Head+1)
		if err != nil {
			return nil, err
		}
		return item, nil
	} else {
		return nil, errors.New("ch not ext")
	}
}

func (q *ChanQueue) PeekStart(chname string) ([]QueueItem, error) {
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if mt, ok := q.mats.Load(chname); ok {
		ma := mt.(*Mat)
		atomic.StoreInt64(&ma.Head, 0)
		atomic.StoreInt64(&ma.Tail, 0)
	} else {
		return nil, errors.New("ch not ext")
	}
	batch := new(leveldb.Batch)
	result := make([]QueueItem, 0)
	iter := q.db.NewIterator(util.BytesPrefix(append([]byte(chname), 0xFF)), q.iteratorOpts)
	for iter.Next() {
		item := QueueItem{}
		item.ID = keyToID(iter.Key())
		item.Key = make([]byte, len(iter.Key()))
		item.Value = make([]byte, len(iter.Value()))
		copy(item.Key, iter.Key())
		copy(item.Value, iter.Value())
		result = append(result, item)
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := q.db.Write(batch, nil)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (q *ChanQueue) getItemByID(chname string, id int64) (*QueueItem, error) {
	mt, ok := q.mats.Load(chname)
	if !ok {
		return nil, leveldb.ErrNotFound
	}
	ma := mt.(*Mat)
	if ma.Tail-ma.Head < 0 {
		return nil, ErrEmpty
	} else if id < ma.Head || id > ma.Tail {
		return nil, ErrOutOfBounds
	}
	var err error
	item := &QueueItem{ID: id, Key: idToKey(chname, id)}
	if item.Value, err = q.db.Get(item.Key, nil); err != nil {
		return nil, err
	}
	return item, nil
}

func (q *ChanQueue) Drop() {
	_ = q.Close()
	_ = os.RemoveAll(q.DataDir)
}

func (q *ChanQueue) Close() error {
	if !q.isOpen {
		return ErrDBClosed
	}
	err := q.db.Close()
	if err != nil {
		return err
	}
	q.isOpen = false
	return nil
}
