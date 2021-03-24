package leveldb

import (
	"bytes"
	//"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/ucwong/bucket/common"
	"time"
)

type LevelDB struct {
	engine *leveldb.DB
}

func New() *LevelDB {
	db := &LevelDB{}
	ldb, err := leveldb.OpenFile(".leveldb", &opt.Options{OpenFilesCacheCapacity: 32})
	if _, iscorrupted := err.(*errors.ErrCorrupted); iscorrupted {
		ldb, err = leveldb.RecoverFile(".leveldb", nil)
	}
	if err != nil {
		return nil
	}
	db.engine = ldb
	return db
}

func (ldb *LevelDB) Get(k []byte) (v []byte) {
	v1, _ := ldb.engine.Get(k, nil)
	v = v1
	return
}

func (ldb *LevelDB) Set(k, v []byte) (err error) {
	err = ldb.engine.Put(k, v, nil)
	return
}

func (ldb *LevelDB) Del(k []byte) (err error) {
	err = ldb.engine.Delete(k, nil)
	return
}

func (ldb *LevelDB) Prefix(k []byte) (res [][]byte) {
	//var tmp []byte
	iter := ldb.engine.NewIterator(util.BytesPrefix(k), nil)
	defer iter.Release()
	for iter.Next() {
		//res = append(res, iter.Value())
		//res = append(res, append(tmp[:0], iter.Value()...))
		res = append(res, common.SafeCopy(nil, iter.Value()))
	}
	return
}

func (ldb *LevelDB) Suffix(k []byte) (res [][]byte) {
	//var tmp []byte
	iter := ldb.engine.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		if bytes.HasSuffix(iter.Key(), k) {
			//fmt.Printf("%s, %s, %s\n", string(iter.Key()), string(iter.Value()), iter.Error())
			//copy(tmp, iter.Value())
			//res = append(res, append(tmp[:0], iter.Value()...))
			res = append(res, common.SafeCopy(nil, iter.Value()))
		}
	}
	return
}

func (ldb *LevelDB) Scan() (res [][]byte) {
	//var tmp []byte
	iter := ldb.engine.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		//fmt.Printf("%s, %s, %s\n", string(iter.Key()), string(iter.Value()), iter.Error())
		//tmp = make([]byte, len(iter.Value()))
		//copy(tmp, iter.Value())
		//res = append(res, append(tmp[:0], iter.Value()...))
		res = append(res, common.SafeCopy(nil, iter.Value()))
	}
	return
}

func (ldb *LevelDB) SetTTL(k, v []byte, expire time.Duration) (err error) {
	return
}

func (ldb *LevelDB) Close() error {
	return ldb.engine.Close()
}
