package yiyidb

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/vmihailenco/msgpack.v2"
	"reflect"
	"strings"
)

func (k *Kvdb) ExistsMix(chname, key string) bool {
	if len(key) > k.maxkv {
		return false
	}
	ok, _ := k.db.Has(idToKeyMix(chname, key), k.iteratorOpts)
	return ok
}

func (k *Kvdb) GetMix(chname, key string) ([]byte, error) {
	if strings.Contains(chname, "-") || strings.Contains(string(key), "-") {
		return nil, errors.New("ch or key has '-'")
	}
	data, err := k.db.Get(idToKeyMix(chname, key), nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (k *Kvdb) GetObjectMix(chname, key string, value interface{}) error {
	data, err := k.Get(idToKeyMix(chname, key))
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

func (k *Kvdb) PutMix(chname, key string, value []byte, ttl int) error {
	if len(value) > k.maxkv {
		return errors.New("out of len")
	}
	if strings.Contains(chname, "-") || strings.Contains(string(key), "-") {
		return errors.New("ch or key has '-'")
	}
	nk := idToKeyMix(chname, key)
	if err := k.db.Put(nk, value, nil); err != nil {
		return err
	}
	if k.enableTtl && ttl > 0 {
		k.ttldb.SetTTL(ttl, nk)
	}
	return nil
}

func (k *Kvdb) PutObjectMix(chname, key string, value interface{}, ttl int) error {
	t := reflect.ValueOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	msg, err := msgpack.Marshal(t.Interface())
	if err != nil {
		return err
	}
	return k.PutMix(chname, key, msg, ttl)
}

func (k *Kvdb) BatPutOrDelMix(chname string, items *[]BatItem) error {
	if strings.Contains(chname, "-") {
		return errors.New("ch or key has '-' ")
	}
	batch := new(leveldb.Batch)
	for _, v := range *items {
		nk := idToKeyMix(chname, string(v.Key))
		switch v.Op {
		case "put":
			if len(v.Key) > k.maxkv || len(v.Value) > k.maxkv {
				return errors.New("out of len")
			}
			batch.Put(nk, v.Value)
			if k.enableTtl && v.Ttl > 0 {
				k.ttldb.SetTTL(v.Ttl, nk)
			}
		case "del":
			if len(v.Key) > k.maxkv {
				return errors.New("out of len")
			}
			batch.Delete(nk)
			if k.enableTtl {
				k.ttldb.DelTTL(nk)
			}
		}
	}
	err := k.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

func (k *Kvdb) DelMix(chname string) error {
	all := k.KeyStartKeys([]byte(chname + "-"))
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

func (k *Kvdb) DelColMix(chname, key string) error {
	if strings.Contains(chname, "-") || strings.Contains(string(key), "-") {
		return errors.New("ch or key has '-' ")
	}
	nk := []byte(idToKeyMix(chname, key))
	err := k.db.Delete(nk, nil)
	if err != nil {
		return err
	}
	if k.enableTtl {
		k.ttldb.DelTTL(nk)
	}
	return nil

}

func (k *Kvdb) AllByObjectMix(chname string, Ntype interface{}) []KvItem {
	nt := reflect.TypeOf(Ntype)
	if nt.Kind() == reflect.Ptr {
		nt = nt.Elem()
	}
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(util.BytesPrefix([]byte(chname+"-")), k.iteratorOpts)
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

func (k *Kvdb) AllByKVMix(chname string) []KvItem {
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

func (k *Kvdb) AllByMixKeys() []map[string]string {
	var keys []map[string]string
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	for iter.Next() {
		c, k := keyToIdMix(iter.Key())
		nk := make(map[string]string)
		nk[c] = k
		keys = append(keys, nk)
	}
	iter.Release()
	return keys
}
