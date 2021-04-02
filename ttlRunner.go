package yiyidb

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"sync"
	"time"
)

type TtlRunner struct {
	DataDir    string
	masterdbs  *sync.Map
	db         *Kvdb
	batchs     map[string]*leveldb.Batch
	localBatch *leveldb.Batch
	inv        int64
}

func OpenTtlRunner(dataDir string) (*TtlRunner, error) {
	var err error
	ttl := &TtlRunner{
		DataDir:    dataDir,
		masterdbs:  &sync.Map{},
		localBatch: &leveldb.Batch{},
		inv:        1000,
	}

	//Open TTl
	ttl.db, err = OpenKvdb(ttl.DataDir, 32)
	if err != nil {
		return nil, err
	}

	go ttl.Run()

	return ttl, nil
}

func (t *TtlRunner) AddTtlDb(ldb *Kvdb, dbname string) error {
	if dbname == "" {
		return errors.New("dbname nil")
	}
	t.masterdbs.Store(dbname, ldb)
	return nil
}

func (t *TtlRunner) Exists(dbname, key string) bool {
	if dbname == "" {
		return false
	}
	db, ok := t.masterdbs.Load(dbname)
	if !ok {
		return false
	}
	return db.(*Kvdb).ExistsMix(dbname, key, nil)
}

func (t *TtlRunner) SetTTL(expires int, dbname, masterDbKey string) error {
	//设置大于0值即设置ttl以秒为单位
	if expires > 0 {
		expiration := time.Now().Add(time.Duration(expires) * time.Second)
		if err := t.db.PutMix(dbname, masterDbKey, IdToKeyPure(expiration.UnixNano()), 0, nil); err != nil {
			return err
		}
	} else if expires < 0 {
		//设置少于0值即取消此记当的TTL属性
		if err := t.db.DelColMix(dbname, masterDbKey, nil); err != nil {
			return err
		}
	}
	return nil
}

func (t *TtlRunner) GetTTL(dbname, masterDbKey string) (float64, error) {
	val, err := t.db.GetMix(dbname, masterDbKey, nil)
	if err != nil {
		return 0, err
	}
	it := time.Unix(0, KeyToIDPure(val))
	return it.Sub(time.Now()).Seconds(), nil
}

func (t *TtlRunner) DelTTL(dbname, masterDbKey string) error {
	return t.db.DelColMix(dbname, masterDbKey, nil)
}

func (t *TtlRunner) Run() {
	for {
		t.localBatch.Reset()
		t.batchs = make(map[string]*leveldb.Batch)
		iter := t.db.newIter(nil, nil)
		now := time.Now()
		for iter.Next() {
			if now.UnixNano() >= KeyToIDPure(iter.Value()) {
				k := iter.Key()
				t.localBatch.Delete(k)
				dbname, masterkey := keyToIdMix(k)
				mdb, ok := t.masterdbs.Load(dbname)
				if !ok {
					continue
				}
				mk := []byte(masterkey)
				master := mdb.(*Kvdb)
				val, err := master.Get(mk, nil)
				if err == nil {
					master.onExp(dbname, mk, val)
				}
				if bat, ok := t.batchs[dbname]; ok {
					bat.Delete(mk)
				} else {
					bat := &leveldb.Batch{}
					bat.Delete(mk)
					t.batchs[dbname] = bat
				}
			}
		}
		iter.Release()
		for dbname, batch := range t.batchs {
			if batch.Len() > 0 {
				mdb, _ := t.masterdbs.Load(dbname)
				_ = mdb.(*Kvdb).exeBatch(batch, nil)
				batch.Reset()
			}
		}
		if t.localBatch.Len() > 0 {
			_ = t.db.exeBatch(t.localBatch, nil)
			t.inv = 1000
		} else {
			use := time.Now().Sub(now).Milliseconds()
			if use <= 0 {
				if t.inv < 2000 {
					t.inv += 500
				}
			} else {
				t.inv = 1000 - use
			}
		}
		time.Sleep(time.Duration(t.inv) * time.Millisecond)
	}
}

func (t *TtlRunner) Close() {
	_ = t.db.Close()
}

func (t *TtlRunner) Drop() {
	_ = t.db.Close()
	_ = os.RemoveAll(t.DataDir)
}
