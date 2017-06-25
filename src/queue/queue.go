package yiyidb

import (
	"sync"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"errors"
	"path/filepath"
	"gopkg.in/vmihailenco/msgpack.v2"
)

const (
	defaultFilterBits int = 10
	KB                int = 1024
	MB                int = KB * 1024
	GB                int = MB * 1024
)

// Type defines the type of data structure used.
type goqueType uint8

// The possible types, used to determine compatibility when
// one stored type is trying to be opened by a different type.
const (
	goqueStack goqueType = iota
	goqueQueue
)

var (
	// ErrIncompatibleType is returned when the opener type is
	// incompatible with the stored type.
	ErrIncompatibleType = errors.New("goque: Opener type is incompatible with stored Goque type")

	// ErrEmpty is returned when the stack or queue is empty.
	ErrEmpty = errors.New("goque: Stack or queue is empty")

	// ErrOutOfBounds is returned when the ID used to lookup an item
	// is outside of the range of the stack or queue.
	ErrOutOfBounds = errors.New("goque: ID used is outside range of stack or queue")

	// ErrDBClosed is returned when the Close function has already
	// been called, causing the stack or queue to close, as well as
	// its underlying database.
	ErrDBClosed = errors.New("goque: Database is closed")
)

// Queue is a standard FIFO (first in, first out) queue.
type Queue struct {
	sync.RWMutex
	DataDir      string
	db           *leveldb.DB
	head         uint64
	tail         uint64
	isOpen       bool
	iteratorOpts *opt.ReadOptions
}

// OpenQueue opens a queue if one exists at the given directory. If one
// does not already exist, a new queue is created.
func OpenQueue(dataDir string) (*Queue, error) {
	var err error

	// Create a new Queue.
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
	opts.Filter = filter.NewBloomFilter(defaultFilterBits)
	opts.Compression = opt.SnappyCompression
	opts.BlockSize = 4 * KB
	opts.WriteBuffer = 4 * MB
	opts.OpenFilesCacheCapacity = 1 * KB
	opts.CompactionTableSize = 32 * MB
	opts.WriteL0SlowdownTrigger = 16
	opts.WriteL0PauseTrigger = 64

	// Open database for the queue.
	q.db, err = leveldb.OpenFile(dataDir, opts)
	if err != nil {
		return nil, err
	}

	// Check if this Goque type can open the requested data directory.
	ok, err := checkGoqueType(dataDir, goqueQueue)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrIncompatibleType
	}
	// Set isOpen and return.
	q.isOpen = true
	return q, q.init()
}

// Enqueue adds an item to the queue.
func (q *Queue) Enqueue(value []byte) (*Item, error) {
	q.Lock()
	defer q.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	// Create new Item.
	item := &Item{
		ID:    q.tail + 1,
		Key:   idToKey(q.tail + 1),
		Value: value,
	}

	// Add it to the queue.
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}

	// Increment tail position.
	q.tail++

	return item, nil
}

// EnqueueString is a helper function for Enqueue that accepts a
// value as a string rather than a byte slice.
func (q *Queue) EnqueueString(value string) (*Item, error) {
	return q.Enqueue([]byte(value))
}

// EnqueueObject is a helper function for Enqueue that accepts any
// value type, which is then encoded into a byte slice using
// encoding/gob.
func (q *Queue) EnqueueObject(value interface{}) (*Item, error) {
	msg, err := msgpack.Marshal(value)
	if err != nil {
		return nil, err
	}
	return q.Enqueue(msg)
}

// Dequeue removes the next item in the queue and returns it.
func (q *Queue) Dequeue() (*Item, error) {
	q.Lock()
	defer q.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	// Try to get the next item in the queue.
	item, err := q.getItemByID(q.head + 1)
	if err != nil {
		return nil, err
	}

	// Remove this item from the queue.
	if err := q.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}

	// Increment head position.
	q.head++

	return item, nil
}

// Peek returns the next item in the queue without removing it.
func (q *Queue) Peek() (*Item, error) {
	q.RLock()
	defer q.RUnlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	return q.getItemByID(q.head + 1)
}

// PeekByOffset returns the item located at the given offset,
// starting from the head of the queue, without removing it.
func (q *Queue) PeekByOffset(offset uint64) (*Item, error) {
	q.RLock()
	defer q.RUnlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	return q.getItemByID(q.head + offset + 1)
}

// PeekByID returns the item with the given ID without removing it.
func (q *Queue) PeekByID(id uint64) (*Item, error) {
	q.RLock()
	defer q.RUnlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	return q.getItemByID(id)
}

// Update updates an item in the queue without changing its position.
func (q *Queue) Update(id uint64, newValue []byte) (*Item, error) {
	q.Lock()
	defer q.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	// Check if item exists in queue.
	if id <= q.head || id > q.tail {
		return nil, ErrOutOfBounds
	}

	// Create new Item.
	item := &Item{
		ID:    id,
		Key:   idToKey(id),
		Value: newValue,
	}

	// Update this item in the queue.
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}

	return item, nil
}

// UpdateString is a helper function for Update that accepts a value
// as a string rather than a byte slice.
func (q *Queue) UpdateString(id uint64, newValue string) (*Item, error) {
	return q.Update(id, []byte(newValue))
}

// UpdateObject is a helper function for Update that accepts any
// value type, which is then encoded into a byte slice using
// encoding/gob.
func (q *Queue) UpdateObject(id uint64, newValue interface{}) (*Item, error) {
	msg, err := msgpack.Marshal(newValue)
	if err != nil {
		return nil, err
	}
	return q.Update(id, msg)
}

// Length returns the total number of items in the queue.
func (q *Queue) Length() uint64 {
	return q.tail - q.head
}

// Close closes the LevelDB database of the queue.
func (q *Queue) Close() {
	q.Lock()
	defer q.Unlock()

	// Check if queue is already closed.
	if !q.isOpen {
		return
	}

	// Reset queue head and tail.
	q.head = 0
	q.tail = 0

	q.db.Close()
	q.isOpen = false
}

// Drop closes and deletes the LevelDB database of the queue.
func (q *Queue) Drop() {
	q.Close()
	os.RemoveAll(q.DataDir)
}

// getItemByID returns an item, if found, for the given ID.
func (q *Queue) getItemByID(id uint64) (*Item, error) {
	// Check if empty or out of bounds.
	if q.Length() == 0 {
		return nil, ErrEmpty
	} else if id <= q.head || id > q.tail {
		return nil, ErrOutOfBounds
	}

	// Get item from database.
	var err error
	item := &Item{ID: id, Key: idToKey(id)}
	if item.Value, err = q.db.Get(item.Key, nil); err != nil {
		return nil, err
	}

	return item, nil
}

// init initializes the queue data.
func (q *Queue) init() error {
	// Create a new LevelDB Iterator.
	iter := q.db.NewIterator(nil, q.iteratorOpts)
	defer iter.Release()

	// Set queue head to the first item.
	if iter.First() {
		q.head = keyToID(iter.Key()) - 1
	}

	// Set queue tail to the last item.
	if iter.Last() {
		q.tail = keyToID(iter.Key())
	}

	return iter.Error()
}

// checkGoqueType checks if the type of data structure
// trying to be opened is compatible with the opener type.
//
// A file named within the data directory used by
// the structure stores the structure type, using the constants
// declared above.
//
// Stacks and Queues are 100% compatible with each other, while
// a PriorityQueue is incompatible with both.
//
// Returns true if types are compatible and false if incompatible.
func checkGoqueType(dataDir string, gt goqueType) (bool, error) {
	// Set the path to file.
	path := filepath.Join(dataDir, "vtype")

	// Read file for this directory.
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return false, err
		}
		defer f.Close()

		// Create byte slice of Type.
		gtb := make([]byte, 1)
		gtb[0] = byte(gt)

		_, err = f.Write(gtb)
		if err != nil {
			return false, err
		}

		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Get the saved type from the file.
	fb := make([]byte, 1)
	_, err = f.Read(fb)
	if err != nil {
		return false, err
	}

	// Convert the file byte to its goqueType.
	filegt := goqueType(fb[0])

	// Compare the types.
	if filegt == gt {
		return true, nil
	} else if filegt == goqueStack && gt == goqueQueue {
		return true, nil
	} else if filegt == goqueQueue && gt == goqueStack {
		return true, nil
	}

	return false, nil
}
