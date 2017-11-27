package yiyidb

import (
	"sync"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"errors"
	"github.com/syndtr/goleveldb/leveldb/util"
	"bytes"
	"os"
	"gopkg.in/vmihailenco/msgpack.v2"
)

//FIFO
type ChanQueue struct {
	sync.RWMutex
	DataDir      string
	db           *leveldb.DB
	isOpen       bool
	iteratorOpts *opt.ReadOptions
	maxkv        int
	mats         map[string]*mat
}

type mat struct {
	mixName string
	head    uint64
	tail    uint64
}

func OpenChanQueue(dataDir string, defaultKeyLen int) (*ChanQueue, error) {
	var err error
	q := &ChanQueue{
		DataDir:      dataDir,
		db:           &leveldb.DB{},
		isOpen:       false,
		mats:         make(map[string]*mat),
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
	return q, q.init()
}

func (q *ChanQueue) init() error {
	iter := q.db.NewIterator(nil, q.iteratorOpts)
	defer iter.Release()
	for iter.Next() {
		mixName := keyName(iter.Key())
		if _, ok := q.mats[mixName]; !ok {
			q.mats[mixName] = &mat{mixName: mixName, head: 0, tail: 0}
		}
	}
	if len(q.mats) > 0 {
		for k, v := range q.mats {
			rg := util.BytesPrefix([]byte(k))
			iter.Seek(rg.Start)
			v.head = keyToID(iter.Key())
			var lastid uint64
			for ok := iter.Seek(rg.Start); ok && bytes.Compare(iter.Key(), rg.Limit) <= 0; ok = iter.Next() {
				lastid = keyToID(iter.Key())
			}
			v.tail = lastid
		}
	}
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
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if len(value) > q.maxkv {
		return nil, errors.New("out of len 512M")
	}
	if mt, ok := q.mats[chname]; ok {
		if err := q.db.Put(idToKey(mt.mixName, mt.tail+1), value, nil); err != nil {
			return nil, err
		}
		mt.tail++
		return &QueueItem{ID: mt.tail, Key: idToKey(chname, mt.tail), Value: value}, nil
	} else {
		item := &QueueItem{ID: 1, Key: idToKey(chname, 1), Value: value}
		if err := q.db.Put(item.Key, item.Value, nil); err != nil {
			return nil, err
		}
		q.mats[chname] = &mat{mixName: chname, tail: item.ID, head: 0}
		return item, nil
	}
}

func (q *ChanQueue) Dequeue(chname string) (*QueueItem, error) {
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if mt, ok := q.mats[chname]; ok {
		item, err := q.getItemByID(chname, mt.head+1)
		if err != nil {
			return nil, err
		}
		if err := q.db.Delete(item.Key, nil); err != nil {
			return nil, err
		}
		mt.head++
		//当队列取空后重置游标
		if mt.head == mt.tail {
			mt.head = 0
			mt.tail = 0
		}
		return item, nil
	} else {
		return nil, errors.New("ch not ext")
	}
}

func (q *ChanQueue) GetMetal(chname string) (uint64, uint64) {
	q.RLock()
	defer q.RUnlock()
	if !q.isOpen {
		return 0, 0
	}
	if mt, ok := q.mats[chname]; ok {
		return mt.head, mt.tail
	} else {
		return 0, 0
	}
}

func (q *ChanQueue) Clear(chname string) error {
	q.RLock()
	defer q.RUnlock()
	if !q.isOpen {
		return ErrDBClosed
	}
	if len(chname) > q.maxkv {
		return errors.New("out of len")
	}
	batch := new(leveldb.Batch)
	iter := q.db.NewIterator(util.BytesPrefix([]byte(chname+"-")), q.iteratorOpts)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := q.db.Write(batch, nil)
	if err != nil {
		return err
	}
	if mt, ok := q.mats[chname]; ok {
		mt.head = 0
		mt.tail = 0
	}
	return nil
}

func (q *ChanQueue) Peek(chname string) (*QueueItem, error) {
	q.RLock()
	defer q.RUnlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if mt, ok := q.mats[chname]; ok {
		item, err := q.getItemByID(chname, mt.head+1)
		if err != nil {
			return nil, err
		}
		return item, nil
	} else {
		return nil, errors.New("ch not ext")
	}
}

func (q *ChanQueue) PeekStart(chname string) ([]*QueueItem, error) {
	q.RLock()
	defer q.RUnlock()
	if !q.isOpen {
		return nil, ErrDBClosed
	}
	if len(chname) > q.maxkv {
		return nil, errors.New("out of len")
	}
	result := make([]*QueueItem, 0)
	iter := q.db.NewIterator(util.BytesPrefix([]byte(chname+"-")), q.iteratorOpts)
	for iter.Next() {
		item := &QueueItem{
			ID:    keyToID(iter.Key()),
			Key:   iter.Key(),
			Value: iter.Value(),
		}
		result = append(result, item)
	}
	iter.Release()
	return result, nil
}

func (q *ChanQueue) getItemByID(chname string, id uint64) (*QueueItem, error) {
	hq := q.mats[chname]
	if hq.tail-hq.head == 0 {
		return nil, ErrEmpty
	} else if id <= hq.head || id > hq.tail {
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
	q.Close()
	os.RemoveAll(q.DataDir)
}

func (q *ChanQueue) Close() {
	q.Lock()
	defer q.Unlock()
	if !q.isOpen {
		return
	}
	q.db.Close()
	q.isOpen = false
}
