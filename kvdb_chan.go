package yiyidb

import (
	"github.com/syndtr/goleveldb/leveldb/util"
	"reflect"
	"gopkg.in/vmihailenco/msgpack.v2"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (k *Kvdb) PutChan(chname string, value []byte, ttl int) error {
	if len(value) > k.maxkv {
		return errors.New("out of len")
	}
	if mt, ok := k.mats[chname]; ok {
		nk := idToKey(chname,mt.tail+1)
		if err := k.db.Put(nk, value, nil); err != nil {
			return err
		}
		if k.enableTtl && ttl > 0 {
			k.ttldb.SetTTL(ttl, nk)
		}
		k.addchan(nk)
		return nil
	} else {
		if err := k.db.Put(idToKey(chname, 1), value, nil); err != nil {
			return err
		}
		k.mats[chname] = &mat{tail: 1, head: 1}
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

func (k *Kvdb) BatPutOrDelChan(chname string, items *[]BatItem) error {
	if k.enableChan {
		h, t := k.getmtinfo(chname)
		batch := new(leveldb.Batch)
		for _, v := range *items {
			switch v.Op {
			case "put":
				if len(v.Key) > k.maxkv || len(v.Value) > k.maxkv {
					return errors.New("out of len")
				}
				batch.Put(v.Key, v.Value)
				if k.enableTtl && v.Ttl > 0 {
					k.ttldb.SetTTL(v.Ttl, v.Key)
				}
				k.addchan(v.Key)
			case "del":
				if len(v.Key) > k.maxkv {
					return errors.New("out of len")
				}
				batch.Delete(v.Key)
				if k.enableTtl {
					k.ttldb.DelTTL(v.Key)
				}
				k.delchan(v.Key)
			}
		}
		err := k.db.Write(batch, nil)
		if err != nil {
			k.setmtinfo(chname, h, t)
			return err
		}
		return nil
	} else {
		return errors.New("kv type not chan")
	}
}

func (k *Kvdb) getmtinfo(chname string) (uint64, uint64) {
	if mt, ok := k.mats[chname]; ok {
		return mt.head, mt.tail
	}
	return 0, 0
}

func (k *Kvdb) setmtinfo(chname string, h, t uint64) {
	if mt, ok := k.mats[chname]; ok {
		mt.head = h
		mt.tail = t
	}
}

func (k *Kvdb) addchan(key []byte) {
	if mt, ok := k.mats[keyName(key)]; ok {
		mt.tail++
		mt.head++
	} else {
		k.mats[keyName(key)] = &mat{tail: 1, head: 1}
	}
}

func (k *Kvdb) Clear(chname string) error {
	all := k.KeyStartKeys([]byte(chname))
	items := make([]BatItem, 0)
	for _, v := range all {
		item := BatItem{
			Op:  "del",
			Key: []byte(v),
		}
		items = append(items, item)
	}
	return k.BatPutOrDel(&items)
}

func (k *Kvdb) delchan(key []byte) {
	if k.enableChan {
		if mt, ok := k.mats[keyName(key)]; ok {
			id := keyToID(key)
			if id <= mt.tail {
				mt.head--
				//当key取空后重置游标
				if mt.head == 0 {
					mt.tail = 0
				}
			}
		}
	}
}

func (k *Kvdb) AllByObjectChan(chname string, Ntype interface{}) []KvItem {
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(util.BytesPrefix([]byte(chname+"-")), k.iteratorOpts)
	for iter.Next() {
		t := reflect.New(reflect.TypeOf(Ntype)).Interface()
		err := msgpack.Unmarshal(iter.Value(), &t)
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

func (k *Kvdb) AllByKVChan(chname string) []KvItem {
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(util.BytesPrefix([]byte(chname+"-")), k.iteratorOpts)
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
		iter := k.db.NewIterator(nil, k.iteratorOpts)
		defer iter.Release()
		for iter.Next() {
			mixName := keyName(iter.Key())
			if _, ok := k.mats[mixName]; !ok {
				k.mats[mixName] = &mat{head: 0, tail: 0}
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
