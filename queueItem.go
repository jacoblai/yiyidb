package yiyidb

import (
	"encoding/binary"
	"gopkg.in/vmihailenco/msgpack.v2"
)


type QueueItem struct {
	ID    uint64
	Key   []byte
	Value []byte
}

func (i *QueueItem) ToString() string {
	return string(i.Value)
}

func (i *QueueItem) ToObject(value interface{}) error {
	err := msgpack.Unmarshal(i.Value, &value)
	return err
}

func idToKey(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}

func keyToID(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}