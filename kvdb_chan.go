package yiyidb

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/vmihailenco/msgpack.v2"
	"reflect"
	"regexp"
	"sync/atomic"
)

func (k *Kvdb) PutChan(chname string, value []byte, ttl int, tran bool) error {
	if mt, ok := k.mats.Load(chname); ok {
		ma := mt.(*Mat)
		atomic.AddInt64(&ma.Tail, 1)
		nk := idToKey(chname, ma.Tail)
		if err := k.Put(nk, value, ttl, tran); err != nil {
			return err
		}
		k.addchan(nk)
	} else {
		if err := k.Put(idToKey(chname, 1), value, ttl, tran); err != nil {
			return err
		}
		k.mats.Store(chname, &Mat{Tail: 1, Head: 1})
	}
	return nil
}

func (k *Kvdb) PutObjectChan(chname string, value interface{}, ttl int, tran bool) error {
	t := reflect.ValueOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	msg, err := msgpack.Marshal(t.Interface())
	if err != nil {
		return err
	}
	return k.PutChan(chname, msg, ttl, tran)
}

func (k *Kvdb) BatPutOrDelChan(chname string, items *[]BatItem) error {
	if k.enableChan {
		h, t := k.getmtinfo(chname)
		batch := new(leveldb.Batch)
		for _, v := range *items {
			switch v.Op {
			case "put":
				batch.Put(v.Key, v.Value)
				if k.enableTtl && v.Ttl > 0 {
					k.ttldb.SetTTL(v.Ttl, v.Key)
				}
				k.addchan(v.Key)
			case "del":
				batch.Delete(v.Key)
				if k.enableTtl {
					k.ttldb.DelTTL(v.Key)
				}
				k.delchan(v.Key)
			}
		}
		if k.tran != nil {
			err := k.tran.Write(batch, nil)
			if err != nil {
				k.setmtinfo(chname, h, t)
				return err
			}
		} else {
			err := k.db.Write(batch, nil)
			if err != nil {
				k.setmtinfo(chname, h, t)
				return err
			}
		}
		return nil
	} else {
		return errors.New("kv type not chan")
	}
}

func (k *Kvdb) getmtinfo(chname string) (int64, int64) {
	if mt, ok := k.mats.Load(chname); ok {
		ma := mt.(*Mat)
		return ma.Head, ma.Tail
	}
	return 0, 0
}

func (k *Kvdb) setmtinfo(chname string, h, t int64) {
	if mt, ok := k.mats.Load(chname); ok {
		ma := mt.(*Mat)
		atomic.StoreInt64(&ma.Head, h)
		atomic.StoreInt64(&ma.Tail, t)
	}
}

func (k *Kvdb) addchan(key []byte) {
	if mt, ok := k.mats.Load(keyName(key)); ok {
		ma := mt.(*Mat)
		atomic.AddInt64(&ma.Head, 1)
		atomic.AddInt64(&ma.Tail, 1)
	} else {
		k.mats.Store(keyName(key), &Mat{Tail: 1, Head: 1})
	}
}

func (k *Kvdb) Clear(chname string, tran bool) error {
	all := k.KeyStartKeys([]byte(chname), tran)
	items := make([]BatItem, 0)
	for _, v := range all {
		item := BatItem{
			Op:  "del",
			Key: []byte(v),
		}
		items = append(items, item)
	}
	return k.BatPutOrDel(&items, tran)
}

func (k *Kvdb) delchan(key []byte) {
	if k.enableChan {
		if mt, ok := k.mats.Load(keyName(key)); ok {
			ma := mt.(*Mat)
			id := keyToID(key)
			if id <= ma.Tail {
				atomic.AddInt64(&ma.Head, -1)
				//当key取空后重置游标
				if ma.Head == 0 {
					ma.Tail = 0
				}
			}
		}
	}
}

func (k *Kvdb) RegexpByObjectChan(chname, exp string, Ntype interface{}, tran bool) ([]KvItem, error) {
	regx, err := regexp.Compile(exp)
	if err != nil {
		return nil, err
	}
	nt := reflect.TypeOf(Ntype)
	if nt.Kind() == reflect.Ptr {
		nt = nt.Elem()
	}
	result := make([]KvItem, 0)
	iter := k.newIter(util.BytesPrefix(append([]byte(chname), 0xFF)), tran)
	for iter.Next() {
		if regx.Match(iter.Key()) {
			t := reflect.New(nt).Interface()
			err := msgpack.Unmarshal(iter.Value(), t)
			if err == nil {
				item := KvItem{}
				item.Key = make([]byte, len(iter.Key()))
				copy(item.Key, iter.Key())
				item.Object = t
				result = append(result, item)
			}
		}
	}
	iter.Release()
	return result, nil
}

func (k *Kvdb) AllByObjectChan(chname string, Ntype interface{}, tran bool) []KvItem {
	nt := reflect.TypeOf(Ntype)
	if nt.Kind() == reflect.Ptr {
		nt = nt.Elem()
	}
	result := make([]KvItem, 0)
	iter := k.newIter(util.BytesPrefix(append([]byte(chname), 0xFF)), tran)
	for iter.Next() {
		t := reflect.New(nt).Interface()
		err := msgpack.Unmarshal(iter.Value(), t)
		if err == nil {
			item := KvItem{}
			item.Key = make([]byte, len(iter.Key()))
			copy(item.Key, iter.Key())
			item.Object = t
			result = append(result, item)
		}
	}
	iter.Release()
	return result
}

func (k *Kvdb) AllByKVChan(chname string, tran bool) []KvItem {
	result := make([]KvItem, 0)
	cha := append([]byte(chname), 0xFF)
	iter := k.newIter(util.BytesPrefix(cha), tran)
	for iter.Next() {
		item := KvItem{}
		item.Key = make([]byte, len(iter.Key()))
		item.Value = make([]byte, len(iter.Value()))
		copy(item.Key, iter.Key())
		copy(item.Value, iter.Value())
		result = append(result, item)
	}
	iter.Release()
	return result
}

func (k *Kvdb) init() {
	if k.enableChan {
		iter := k.newIter(nil, false)
		defer iter.Release()
		for iter.Next() {
			mixName := keyName(iter.Key())
			if _, ok := k.mats.Load(mixName); !ok {
				k.mats.Store(mixName, &Mat{Head: -1, Tail: 0})
			}
		}
		k.mats.Range(func(nk, v interface{}) bool {
			var lastid int64
			ma := v.(*Mat)
			iter.Seek(append(nk.([]byte), 0xFF))
			for iter.Next() {
				atomic.AddInt64(&ma.Head, 1)
				lastid = keyToID(iter.Key())
			}
			atomic.StoreInt64(&ma.Tail, lastid)
			return true
		})
	}
}
