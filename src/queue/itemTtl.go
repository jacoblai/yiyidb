package yiyidb

import (
	"sync"
	"time"
)

type ItemTtl struct {
	sync.RWMutex
	Dkey    []byte
	Expires *time.Time
}

func (item *ItemTtl) touch(duration time.Duration) {
	item.Lock()
	expiration := time.Now().Add(duration)
	item.Expires = &expiration
	item.Unlock()
}

func (item *ItemTtl) expired() bool {
	value := false
	item.RLock()
	if item.Expires == nil {
		value = true
	} else {
		value = time.Now().After(*item.Expires)
	}
	item.RUnlock()
	return value
}