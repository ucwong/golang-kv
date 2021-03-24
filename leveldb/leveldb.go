package leveldb

import (
	"bytes"
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
	iter := ldb.engine.NewIterator(util.BytesPrefix(k), nil)
	defer iter.Release()
	for iter.Next() {
		res = append(res, common.SafeCopy(nil, iter.Value()))
	}
	return
}

func (ldb *LevelDB) Suffix(k []byte) (res [][]byte) {
	iter := ldb.engine.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		if bytes.HasSuffix(iter.Key(), k) {
			res = append(res, common.SafeCopy(nil, iter.Value()))
		}
	}
	return
}

func (ldb *LevelDB) Range(start, limit []byte) (res [][]byte) {
	iter := ldb.engine.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	defer iter.Release()
	for iter.Next() {
		res = append(res, common.SafeCopy(nil, iter.Value()))
	}
	return
}

func (ldb *LevelDB) Scan() (res [][]byte) {
	iter := ldb.engine.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
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
