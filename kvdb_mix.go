package yiyidb

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/vmihailenco/msgpack.v2"
	"reflect"
)

func (k *Kvdb) ExistsMix(chname, key string) bool {
	if k.tran != nil {
		ok, _ := k.tran.Has(idToKeyMix(chname, key), k.iteratorOpts)
		return ok
	} else {
		ok, _ := k.db.Has(idToKeyMix(chname, key), k.iteratorOpts)
		return ok
	}
}

func (k *Kvdb) GetMix(chname, key string) ([]byte, error) {
	if k.tran != nil {
		data, err := k.tran.Get(idToKeyMix(chname, key), nil)
		if err != nil {
			return nil, err
		}
		return data, nil
	} else {
		data, err := k.db.Get(idToKeyMix(chname, key), nil)
		if err != nil {
			return nil, err
		}
		return data, nil
	}
}

func (k *Kvdb) GetObjectMix(chname, key string, value interface{}, tran bool) error {
	data, err := k.Get(idToKeyMix(chname, key), tran)
	if err != nil {
		return err
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Ptr {
		return errors.New("not ptr")
	}
	err = msgpack.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	return nil
}

func (k *Kvdb) PutMix(chname, key string, value []byte, ttl int, tran bool) error {
	nk := idToKeyMix(chname, key)
	if err := k.Put(nk, value, ttl, tran); err != nil {
		return err
	}
	return nil
}

func (k *Kvdb) PutObjectMix(chname, key string, value interface{}, ttl int, tran bool) error {
	t := reflect.ValueOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	msg, err := msgpack.Marshal(t.Interface())
	if err != nil {
		return err
	}
	return k.PutMix(chname, key, msg, ttl, tran)
}

func (k *Kvdb) BatPutOrDelMix(chname string, items *[]BatItem) error {
	batch := new(leveldb.Batch)
	for _, v := range *items {
		nk := idToKeyMix(chname, string(v.Key))
		switch v.Op {
		case "put":
			batch.Put(nk, v.Value)
			if k.enableTtl && v.Ttl > 0 {
				k.ttldb.SetTTL(v.Ttl, nk)
			}
		case "del":
			batch.Delete(nk)
			if k.enableTtl {
				k.ttldb.DelTTL(nk)
			}
		}
	}
	if k.tran != nil {
		err := k.tran.Write(batch, nil)
		if err != nil {
			return err
		}
	} else {
		err := k.db.Write(batch, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *Kvdb) DelMix(chname string, tran bool) error {
	all := k.KeyStartKeys(append([]byte(chname), 0xFF), tran)
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

func (k *Kvdb) DelColMix(chname, key string, tran bool) error {
	nk := idToKeyMix(chname, key)
	err := k.Del(nk, tran)
	if err != nil {
		return err
	}
	return nil

}

func (k *Kvdb) AllByObjectMix(chname string, Ntype interface{}, tran bool) []KvItem {
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

func (k *Kvdb) AllByKVMix(chname string, tran bool) []KvItem {
	result := make([]KvItem, 0)
	iter := k.newIter(util.BytesPrefix(append([]byte(chname), 0xFF)), tran)
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

func (k *Kvdb) AllByMixKeys(tran bool) []map[string]string {
	var keys []map[string]string
	iter := k.newIter(nil, tran)
	for iter.Next() {
		c, k := keyToIdMix(iter.Key())
		nk := make(map[string]string)
		nk[c] = k
		keys = append(keys, nk)
	}
	iter.Release()
	return keys
}
