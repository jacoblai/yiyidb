package yiyidb

import (
	"encoding/binary"
	"gopkg.in/vmihailenco/msgpack.v2"
	"bytes"
	"errors"
)

const (
	KB int = 1024
	MB int = KB * 1024
	GB int = MB * 1024
)

var (
	ErrEmpty       = errors.New("queue is empty")
	ErrOutOfBounds = errors.New("ID used is outside range of queue")
	ErrDBClosed    = errors.New("Database is closed")
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

func idToKeyPure(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}

func keyToIDPure(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}

func idToKey(chname string, id uint64) []byte {
	kid := make([]byte, 8)
	binary.BigEndian.PutUint64(kid, id)
	return append([]byte(chname+"-"), kid...)
}

func keyName(key []byte) string {
	k := bytes.Split(key, []byte("-"))
	if len(k) == 2 {
		return string(k[0])
	}
	return ""
}

func keyToID(key []byte) uint64 {
	k := bytes.Split(key, []byte("-"))
	if len(k) == 2 {
		return binary.BigEndian.Uint64(k[1])
	}
	return 0
}
