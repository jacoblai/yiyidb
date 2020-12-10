package yiyidb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"gopkg.in/vmihailenco/msgpack.v2"
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
	ID    int64
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

func IdToKeyPure(id int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(id))
	return key
}

func KeyToIDPure(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key))
}

func idToKey(chname string, id int64) []byte {
	kid := make([]byte, 8)
	binary.BigEndian.PutUint64(kid, uint64(id))
	chn := append([]byte(chname), 0xFF)
	return append(chn, kid...)
}

func idToKeyMix(chname, key string) []byte {
	chn := append([]byte(chname), 0xFF)
	return append(chn, []byte(key)...)
}

func keyToIdMix(mixkey []byte) (string, string) {
	bts := bytes.Split(mixkey, []byte{0xFF})
	return string(bts[0]), string(bts[1])
}

func keyName(key []byte) string {
	k := bytes.Split(key, []byte{0xFF})
	if len(k) == 2 {
		return string(k[0])
	}
	return ""
}

func keyToID(key []byte) int64 {
	k := bytes.Split(key, []byte{0xFF})
	if len(k) == 2 {
		return int64(binary.BigEndian.Uint64(k[1]))
	}
	return 0
}

func IdToKeyPureUint16(id uint16) []byte {
	key := make([]byte, 2)
	binary.BigEndian.PutUint16(key, id)
	return key
}

func KeyToIDPureUint16(key []byte) uint16 {
	return binary.BigEndian.Uint16(key)
}
