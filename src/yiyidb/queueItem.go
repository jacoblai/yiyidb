package yiyidb

import (
	"encoding/binary"
	"gopkg.in/vmihailenco/msgpack.v2"
)


// Item represents an entry in either a stack or queue.
type QueueItem struct {
	ID    uint64
	Key   []byte
	Value []byte
}

// ToString returns the item value as a string.
func (i *QueueItem) ToString() string {
	return string(i.Value)
}

// ToObject decodes the item value into the given value type using
// encoding/gob.
//
// The value passed to this method should be a pointer to a variable
// of the type you wish to decode into. The variable pointed to will
// hold the decoded object.
func (i *QueueItem) ToObject(value interface{}) error {
	err := msgpack.Unmarshal(i.Value, &value)
	return err
}

// idToKey converts and returns the given ID to a key.
func idToKey(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}

// keyToID converts and returns the given key to an ID.
func keyToID(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}