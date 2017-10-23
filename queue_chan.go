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
		mixName := q.keyName(iter.Key())
		if _, ok := q.mats[mixName]; !ok {
			q.mats[mixName] = &mat{mixName: mixName, head: 0, tail: 0}
		}
	}
	if len(q.mats) > 0 {
		for k, v := range q.mats {
			rg := util.BytesPrefix([]byte(k))
			iter.Seek(rg.Start)
			v.head = q.keyToID(iter.Key())
			var lastid uint64
			for ok := iter.Seek(rg.Start); ok && bytes.Compare(iter.Key(), rg.Limit) <= 0; ok = iter.Next() {
				lastid = q.keyToID(iter.Key())
			}
			v.tail = lastid
		}
	}
	return iter.Error()
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
		if err := q.db.Put(q.idToKey(mt.mixName, mt.tail+1), value, nil); err != nil {
			return nil, err
		}
		mt.tail++
		return &QueueItem{ID: mt.tail, Key: q.idToKey(chname, mt.tail), Value: value}, nil
	} else {
		item := &QueueItem{ID: 1, Key: q.idToKey(chname, 1), Value: value}
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

func (q *ChanQueue) getItemByID(chname string, id uint64) (*QueueItem, error) {
	hq := q.mats[chname]
	if hq.tail-hq.head == 0 {
		return nil, ErrEmpty
	} else if id <= hq.head || id > hq.tail {
		return nil, ErrOutOfBounds
	}
	var err error
	item := &QueueItem{ID: id, Key: q.idToKey(chname, id)}
	if item.Value, err = q.db.Get(item.Key, nil); err != nil {
		return nil, err
	}
	return item, nil
}

func (q *ChanQueue) idToKey(chname string, id uint64) []byte {
	kid := make([]byte, 8)
	binary.BigEndian.PutUint64(kid, id)
	return append([]byte(chname+"-"), kid...)
}

func (q *ChanQueue) keyName(key []byte) string {
	k := bytes.Split(key, []byte("-"))
	if len(k) == 2 {
		return string(k[0])
	}
	return ""
}

func (q *ChanQueue) keyToID(key []byte) uint64 {
	k := bytes.Split(key, []byte("-"))
	if len(k) == 2 {
		return binary.BigEndian.Uint64(k[1])
	}
	return 0
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
