// Copyright (C) 2022 ucwong
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>

package pebble

import (
	"bytes"
	//"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ucwong/go-ttlmap"
	"github.com/ucwong/golang-kv/common"
)

type Pebble struct {
	engine  *pebble.DB
	ttl_map *ttlmap.Map
	wb      *pebble.Batch
	once    sync.Once
}

type PebbleOption func(pebble.Options) pebble.Options

func Open(path string, opts ...PebbleOption) *Pebble {
	//if len(path) == 0 {
	path = filepath.Join(path, common.GLOBAL_SPACE, ".pebble")
	//}
	db := &Pebble{}
	option := pebble.Options{}
	for _, op := range opts {
		option = op(option)
	}
	ldb, err := pebble.Open(path, &option)
	//if _, iscorrupted := err.(*errors.ErrCorrupted); iscorrupted {
	//	ldb, err = pebble.RecoverFile(path, nil)
	//}
	if err != nil {
		//panic(err)
		return nil
	}
	db.engine = ldb
	db.wb = new(pebble.Batch)

	options := &ttlmap.Options{
		InitialCapacity: 1024 * 1024,
		OnWillExpire: func(key string, item ttlmap.Item) {
			//fmt.Printf("expired: [%s=%v]\n", key, item.Value())
			//b.Del([]byte(key))
		},
		OnWillEvict: func(key string, item ttlmap.Item) {
			//fmt.Printf("evicted: [%s=%v]\n", key, item.Value())
			//db.Del([]byte(key))
			db.engine.Delete([]byte(key), nil)
		},
	}
	db.ttl_map = ttlmap.New(options)
	return db
}

func (ldb *Pebble) Get(k []byte) (v []byte) {
	item, err := ldb.ttl_map.Get(string(k))
	if err == nil {
		return []byte(item.Value().(string))
	}

	v, closer, err := ldb.engine.Get(k)
	if err == nil {
		defer closer.Close()
	}
	return
}

func (ldb *Pebble) Set(k, v []byte) (err error) {
	if _, err = ldb.ttl_map.Delete(string(k)); err != nil {
		return
	}

	err = ldb.engine.Set(k, v, pebble.Sync)
	return
}

func (ldb *Pebble) Del(k []byte) (err error) {
	if _, err = ldb.ttl_map.Delete(string(k)); err != nil {
		return
	}

	err = ldb.engine.Delete(k, nil)
	return
}

func (ldb *Pebble) Prefix(k []byte) (res [][]byte) {
	keyUpperBound := func(b []byte) []byte {
		end := make([]byte, len(b))
		copy(end, b)
		for i := len(end) - 1; i >= 0; i-- {
			end[i] = end[i] + 1
			if end[i] != 0 {
				return end[:i+1]
			}
		}
		return nil // no upper-bound
	}
	prefixIterOptions := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: keyUpperBound(prefix),
		}
	}

	iter := ldb.engine.NewIter(prefixIterOptions(k))
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		res = append(res, common.SafeCopy(nil, iter.Value()))
	}
	return
}

func (ldb *Pebble) Suffix(k []byte) (res [][]byte) {
	iter := ldb.engine.NewIter(nil)
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if bytes.HasSuffix(iter.Key(), k) {
			res = append(res, common.SafeCopy(nil, iter.Value()))
		}
	}
	return
}

func (ldb *Pebble) Range(start, limit []byte) (res [][]byte) {
	return
}

func (ldb *Pebble) Scan() (res [][]byte) {
	iter := ldb.engine.NewIter(nil)
	defer iter.Close()
	for iter.Next() {
		res = append(res, common.SafeCopy(nil, iter.Value()))
	}
	return
}

func (ldb *Pebble) SetTTL(k, v []byte, expire time.Duration) (err error) {
	if err = ldb.ttl_map.Set(string(k), ttlmap.NewItem(string(v), ttlmap.WithTTL(expire)), nil); err != nil {
		return
	}

	err = ldb.engine.Set(k, v, pebble.Sync)

	if err != nil {
		// TODO
		ldb.ttl_map.Delete(string(k))
	}

	return
}

func (ldb *Pebble) Close() error {
	ldb.once.Do(func() {
		ldb.ttl_map.Drain()
	})
	return ldb.engine.Close()
}

func (ldb *Pebble) BatchSet(kvs map[string][]byte) error {
	for k, v := range kvs {
		ldb.wb.Set([]byte(k), v, nil)
	}
	return ldb.engine.Flush()
}
