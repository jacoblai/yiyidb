package yiyidb

import (
	"github.com/syndtr/goleveldb/leveldb/util"
	"reflect"
	"gopkg.in/vmihailenco/msgpack.v2"
	"errors"
)

func (k *Kvdb) PutChan(chname string, value []byte, ttl int) error {
	if len(value) > k.maxkv {
		return errors.New("out of len")
	}
	if mt, ok := k.mats[chname]; ok {
		if err := k.db.Put(idToKey(chname, mt.tail+1), value, nil); err != nil {
			return err
		}
		mt.tail++
		mt.head++
		if k.enableTtl && ttl > 0 {
			k.ttldb.SetTTL(ttl, idToKey(chname, mt.tail+1))
		}
		return nil
	} else {
		if err := k.db.Put(idToKey(chname, 1), value, nil); err != nil {
			return err
		}
		k.mats[chname] = &mat{mixName: chname, tail: 1, head: 1}
		return nil
	}
	return nil
}

func (k *Kvdb) PutObjectChan(chname string, value interface{}, ttl int) error {
	t := reflect.ValueOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	msg, err := msgpack.Marshal(t.Interface())
	if err != nil {
		return err
	}
	return k.PutChan(chname, msg, ttl)
}

func (k *Kvdb) AllByObjectChan(chname string, Ntype interface{}) []KvItem {
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(util.BytesPrefix([]byte(chname+"-")), k.iteratorOpts)
	for iter.Next() {
		t := reflect.New(reflect.TypeOf(Ntype)).Interface()
		err := msgpack.Unmarshal(iter.Value(), &t)
		if err == nil {
			item := KvItem{}
			item.Key = make([]byte,len(iter.Key()))
			copy(item.Key, iter.Key())
			item.Object = t
			result = append(result, item)
		}
	}
	iter.Release()
	return result
}

func (k *Kvdb) AllByKVChan(chname string) []KvItem {
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(util.BytesPrefix([]byte(chname+"-")), k.iteratorOpts)
	for iter.Next() {
		item := KvItem{}
		item.Key = make([]byte,len(iter.Key()))
		item.Value = make([]byte,len(iter.Value()))
		copy(item.Key, iter.Key())
		copy(item.Value, iter.Value())
		result = append(result, item)
	}
	iter.Release()
	return result
}

func (k *Kvdb) DelChan(chname string, key []byte) error {
	if len(key) > k.maxkv {
		return errors.New("out of len")
	}
	if mt, ok := k.mats[chname]; ok {
		id := keyToID(key)
		if id <= mt.tail{
			err := k.db.Delete(key, nil)
			if err != nil {
				return err
			}
			if k.enableTtl {
				k.ttldb.DelTTL(key)
			}
			mt.head--
			//当key取空后重置游标
			if mt.head == 0 {
				mt.tail = 0
			}
		}
	}
	return nil
}

func (k *Kvdb) init() {
	if k.enableChan{
		iter := k.db.NewIterator(nil, k.iteratorOpts)
		defer iter.Release()
		for iter.Next() {
			mixName := keyName(iter.Key())
			if _, ok := k.mats[mixName]; !ok {
				k.mats[mixName] = &mat{mixName: mixName, head: 0, tail: 0}
			}
		}
		if len(k.mats) > 0 {
			for nk, v := range k.mats {
				var lastid uint64
				iter := k.db.NewIterator(util.BytesPrefix([]byte(nk+"-")), k.iteratorOpts)
				for iter.Next() {
					v.head++
					lastid = keyToID(iter.Key())
				}
				iter.Release()
				v.tail = lastid
			}
		}
	}
}
