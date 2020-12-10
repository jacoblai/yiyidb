package yiyidb

import (
	"bytes"
	"errors"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/vmihailenco/msgpack.v2"
	"math"
	"os"
	"reflect"
	"regexp"
	"sync"
)

type Kvdb struct {
	sync.RWMutex
	DataDir      string
	db           *leveldb.DB
	ttldb        *ttlRunner
	enableTtl    bool
	enableChan   bool
	mats         map[string]*mat
	maxkv        int
	iteratorOpts *opt.ReadOptions
	OnExpirse    func(key, value []byte)
}

type KvItem struct {
	Key    []byte
	Value  []byte
	Object interface{}
}

func OpenKvdb(dataDir string, nChan, nttl bool, defaultKeyLen int) (*Kvdb, error) {
	var err error

	kv := &Kvdb{
		DataDir:      dataDir,
		db:           &leveldb.DB{},
		iteratorOpts: &opt.ReadOptions{DontFillCache: true},
		enableTtl:    nttl,
		enableChan:   nChan,
		mats:         make(map[string]*mat),
		maxkv:        256 * MB,
	}

	bloom := Precision(float64(defaultKeyLen)*1.44, 0, true)

	opts := &opt.Options{}
	opts.ErrorIfMissing = false
	opts.BlockCacheCapacity = 4 * MB
	opts.Filter = filter.NewBloomFilter(int(bloom))
	opts.Compression = opt.SnappyCompression
	opts.BlockSize = 4 * KB
	opts.WriteBuffer = 4 * MB
	opts.OpenFilesCacheCapacity = 1 * KB
	opts.CompactionTableSize = 32 * MB
	opts.WriteL0SlowdownTrigger = 16
	opts.WriteL0PauseTrigger = 64

	// Open database for the queue.
	kv.db, err = leveldb.OpenFile(kv.DataDir, opts)
	if err != nil {
		return nil, err
	}

	if kv.enableTtl {
		//Open TTl
		kv.ttldb, err = OpenTtlRunner(kv.db, kv.DataDir, int(bloom))
		if err != nil {
			return nil, err
		}
		kv.ttldb.HandleExpirse = kv.onExp
		//run ttl func
		go kv.ttldb.Run()
	}

	kv.init()

	return kv, nil
}

func Precision(f float64, prec int, round bool) float64 {
	pow10N := math.Pow10(prec)
	if round {
		return (math.Trunc(f+0.5/pow10N) * pow10N) / pow10N
	}
	return math.Trunc((f)*pow10N) / pow10N
}

func (k *Kvdb) Drop() {
	_ = k.Close()
	_ = os.RemoveAll(k.DataDir)
}

func (k *Kvdb) onExp(key, value []byte) {
	if k.OnExpirse != nil {
		k.OnExpirse(key, value)
	}
}

func (k *Kvdb) NilTTL(key []byte) error {
	if len(key) > k.maxkv {
		return errors.New("out of len")
	}
	if k.enableTtl && k.ttldb.Exists(key) {
		return k.ttldb.SetTTL(-1, key)
	} else {
		return errors.New("ttl not found")
	}
}

func (k *Kvdb) SetTTL(key []byte, ttl int) error {
	if len(key) > k.maxkv {
		return errors.New("out of len")
	}
	if k.enableTtl && k.Exists(key) {
		if ttl > 0 {
			return k.ttldb.SetTTL(ttl, key)
		} else {
			return errors.New("must > 0")
		}
	} else {
		return errors.New("records not found")
	}
}

func (k *Kvdb) GetTTL(key []byte) (float64, error) {
	if len(key) > k.maxkv {
		return 0, errors.New("out of len")
	}
	if k.enableTtl {
		return k.ttldb.GetTTL(key)
	} else {
		return 0, errors.New("ttl not enable")
	}
}

func (k *Kvdb) Exists(key []byte) bool {
	if len(key) > k.maxkv {
		return false
	}
	ok, _ := k.db.Has(key, k.iteratorOpts)
	return ok
}

func (k *Kvdb) Get(key []byte) ([]byte, error) {
	if len(key) > k.maxkv {
		return nil, errors.New("out of len")
	}
	data, err := k.db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (k *Kvdb) GetObject(key []byte, value interface{}) error {
	data, err := k.Get(key)
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

func (k *Kvdb) GetObjectFirst(value interface{}) ([]byte, error) {
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	defer iter.Release()
	if !iter.First() {
		return nil, errors.New("last op error")
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Ptr {
		return nil, errors.New("not ptr")
	}
	err := msgpack.Unmarshal(iter.Value(), &value)
	if err != nil {
		return nil, err
	}
	return iter.Key(), nil
}

func (k *Kvdb) GetObjectLast(value interface{}) ([]byte, error) {
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	defer iter.Release()
	if !iter.Last() {
		return nil, errors.New("last op error")
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Ptr {
		return nil, errors.New("not ptr")
	}
	err := msgpack.Unmarshal(iter.Value(), &value)
	if err != nil {
		return nil, err
	}
	return iter.Key(), nil
}

func (k *Kvdb) GetJson(key []byte, value interface{}) error {
	data, err := k.Get(key)
	if err != nil {
		return err
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Ptr {
		return errors.New("not ptr")
	}
	err = ffjson.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	return nil
}

func (k *Kvdb) GetFirst() ([]byte, []byte, error) {
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	defer iter.Release()
	if !iter.First() {
		return nil, nil, errors.New("op error")
	}
	return iter.Key(), iter.Value(), nil
}

func (k *Kvdb) GetLast() ([]byte, []byte, error) {
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	defer iter.Release()
	if !iter.Last() {
		return nil, nil, errors.New("op error")
	}
	return iter.Key(), iter.Value(), nil
}

func (k *Kvdb) GetJsonFirst(value interface{}) ([]byte, error) {
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	defer iter.Release()
	if !iter.First() {
		return nil, errors.New("op error")
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Ptr {
		return nil, errors.New("not ptr")
	}
	err := ffjson.Unmarshal(iter.Value(), &value)
	if err != nil {
		return nil, err
	}
	return iter.Key(), nil
}

func (k *Kvdb) GetJsonLast(value interface{}) ([]byte, error) {
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	defer iter.Release()
	if !iter.Last() {
		return nil, errors.New("op error")
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Ptr {
		return nil, errors.New("not ptr")
	}
	err := ffjson.Unmarshal(iter.Value(), &value)
	if err != nil {
		return nil, err
	}
	return iter.Key(), nil
}

func (k *Kvdb) GetLastKey() ([]byte, error) {
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	defer iter.Release()
	if !iter.Last() {
		return nil, errors.New("last op error")
	}
	return iter.Key(), nil
}

func (k *Kvdb) Put(key, value []byte, ttl int) error {
	if len(key) > k.maxkv || len(value) > k.maxkv {
		return errors.New("out of len")
	}
	err := k.db.Put(key, value, nil)
	if err != nil {
		return err
	}
	if k.enableTtl && ttl > 0 {
		k.ttldb.SetTTL(ttl, key)
	}
	return nil
}

func (k *Kvdb) OpenTransaction() (*leveldb.Transaction, error) {
	return k.db.OpenTransaction()
}

func (k *Kvdb) PutObject(key []byte, value interface{}, ttl int) error {
	t := reflect.ValueOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	msg, err := msgpack.Marshal(t.Interface())
	if err != nil {
		return err
	}
	return k.Put(key, msg, ttl)
}

func (k *Kvdb) PutJson(key []byte, value interface{}, ttl int) error {
	t := reflect.ValueOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	msg, err := ffjson.Marshal(t.Interface())
	if err != nil {
		return err
	}
	return k.Put(key, msg, ttl)
}

func (k *Kvdb) BatPutOrDel(items *[]BatItem) error {
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
		case "del":
			if len(v.Key) > k.maxkv {
				return errors.New("out of len")
			}
			batch.Delete(v.Key)
			if k.enableTtl {
				k.ttldb.DelTTL(v.Key)
			}
		}
	}
	err := k.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

func (k *Kvdb) Del(key []byte) error {
	if len(key) > k.maxkv {
		return errors.New("out of len")
	}
	err := k.db.Delete(key, nil)
	if err != nil {
		return err
	}
	if k.enableTtl {
		k.ttldb.DelTTL(key)
	}
	k.delchan(key)
	return nil
}

func (k *Kvdb) AllByObject(Ntype interface{}) []KvItem {
	nt := reflect.TypeOf(Ntype)
	if nt.Kind() == reflect.Ptr {
		nt = nt.Elem()
	}
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(nil, k.iteratorOpts)
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

func (k *Kvdb) AllByJson(Ntype interface{}) []KvItem {
	nt := reflect.TypeOf(Ntype)
	if nt.Kind() == reflect.Ptr {
		nt = nt.Elem()
	}
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	for iter.Next() {
		t := reflect.New(nt).Interface()
		err := ffjson.Unmarshal(iter.Value(), t)
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

func (k *Kvdb) AllByKV() []KvItem {
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(nil, k.iteratorOpts)
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

func (k *Kvdb) AllKeys() []string {
	var keys []string
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Release()
	return keys
}

func (k *Kvdb) RegexpKeys(exp string) ([]string, error) {
	regx, err := regexp.Compile(exp)
	if err != nil {
		return nil, err
	}
	var keys []string
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	for iter.Next() {
		if regx.Match(iter.Key()) {
			keys = append(keys, string(iter.Key()))
		}
	}
	iter.Release()
	return keys, nil
}

func (k *Kvdb) RegexpByKV(exp string) ([]KvItem, error) {
	regx, err := regexp.Compile(exp)
	if err != nil {
		return nil, err
	}
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	for iter.Next() {
		if regx.Match(iter.Key()) {
			item := KvItem{}
			item.Key = make([]byte, len(iter.Key()))
			item.Value = make([]byte, len(iter.Value()))
			copy(item.Key, iter.Key())
			copy(item.Value, iter.Value())
			result = append(result, item)
		}
	}
	iter.Release()
	return result, nil
}

func (k *Kvdb) KeyStartDels(key []byte) error {
	batch := new(leveldb.Batch)
	iter := k.db.NewIterator(util.BytesPrefix(key), k.iteratorOpts)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := k.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

func (k *Kvdb) KeyStartKeys(key []byte) []string {
	var keys []string
	iter := k.db.NewIterator(util.BytesPrefix(key), k.iteratorOpts)
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Release()
	return keys
}

func (k *Kvdb) Iter() iterator.Iterator {
	return k.db.NewIterator(nil, k.iteratorOpts)
}

func (k *Kvdb) IterStartWith(key []byte) (iterator.Iterator, error) {
	if len(key) > k.maxkv {
		return nil, errors.New("out of len")
	}
	return k.db.NewIterator(util.BytesPrefix(key), k.iteratorOpts), nil
}

func (k *Kvdb) IterRelease(iter iterator.Iterator) {
	iter.Release()
}

func (k *Kvdb) KeyStart(key []byte) ([]KvItem, error) {
	if len(key) > k.maxkv {
		return nil, errors.New("out of len")
	}
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(util.BytesPrefix(key), k.iteratorOpts)
	for iter.Next() {
		item := KvItem{}
		item.Key = make([]byte, len(iter.Key()))
		item.Value = make([]byte, len(iter.Value()))
		copy(item.Key, iter.Key())
		copy(item.Value, iter.Value())
		result = append(result, item)
	}
	iter.Release()
	return result, nil
}

func (k *Kvdb) KeyStartByObject(key []byte, Ntype interface{}) ([]KvItem, error) {
	if len(key) > k.maxkv {
		return nil, errors.New("out of len")
	}
	nt := reflect.TypeOf(Ntype)
	if nt.Kind() == reflect.Ptr {
		nt = nt.Elem()
	}
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(util.BytesPrefix(key), k.iteratorOpts)
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
	return result, nil
}

func (k *Kvdb) RegexpByObject(exp string, Ntype interface{}) ([]KvItem, error) {
	regx, err := regexp.Compile(exp)
	if err != nil {
		return nil, err
	}
	nt := reflect.TypeOf(Ntype)
	if nt.Kind() == reflect.Ptr {
		nt = nt.Elem()
	}
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(nil, k.iteratorOpts)
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

func (k *Kvdb) KeyRange(min, max []byte) ([]KvItem, error) {
	if len(min) > k.maxkv || len(max) > k.maxkv {
		return nil, errors.New("out of len")
	}
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	for ok := iter.Seek(min); ok && bytes.Compare(iter.Key(), max) <= 0; ok = iter.Next() {
		item := KvItem{}
		item.Key = make([]byte, len(iter.Key()))
		item.Value = make([]byte, len(iter.Value()))
		copy(item.Key, iter.Key())
		copy(item.Value, iter.Value())
		result = append(result, item)
	}
	iter.Release()
	return result, nil
}

func (k *Kvdb) KeyRangeByObject(min, max []byte, Ntype interface{}) ([]KvItem, error) {
	if len(min) > k.maxkv || len(max) > k.maxkv {
		return nil, errors.New("out of len")
	}
	nt := reflect.TypeOf(Ntype)
	if nt.Kind() == reflect.Ptr {
		nt = nt.Elem()
	}
	result := make([]KvItem, 0)
	iter := k.db.NewIterator(nil, k.iteratorOpts)
	for ok := iter.Seek(min); ok && bytes.Compare(iter.Key(), max) <= 0; ok = iter.Next() {
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
	return result, nil
}

func (k *Kvdb) Close() error {
	err := k.db.Close()
	if err != nil {
		return err
	}
	if k.enableTtl {
		k.ttldb.Close()
	}
	return nil
}
