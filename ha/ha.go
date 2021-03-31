package ha

import (
	"github.com/ucwong/golang-kv/badger"
	"github.com/ucwong/golang-kv/bolt"
	"github.com/ucwong/golang-kv/leveldb"
	"time"
)

type Ha struct {
	ldb *leveldb.LevelDB
	bgr *badger.Badger
	bot *bolt.Bolt
}

const GLOBAL = "m41gA7omIWU4s"

func Open(path string) *Ha {
	if len(path) == 0 {
		path = ".ha"
	}
	ha := &Ha{}
	ha.bot = bolt.Open(path + ".bot")
	ha.bgr = badger.Open(path + ".badger")
	ha.ldb = leveldb.Open(path + ".leveldb")

	return ha
}

func (b *Ha) Get(k []byte) (v []byte) {
	v = b.bot.Get(k)
	if v == nil {
		v = b.bgr.Get(k)
	}

	if v == nil {
		v = b.ldb.Get(k)
	}
	return
}

func (b *Ha) Set(k, v []byte) (err error) {
	go b.bot.Set(k, v)
	go b.bgr.Set(k, v)
	return b.ldb.Set(k, v)
}

func (b *Ha) Del(k []byte) (err error) {
	go b.bot.Del(k)
	go b.bgr.Del(k)
	return b.ldb.Del(k)
}

func (b *Ha) Prefix(prefix []byte) (res [][]byte) {
	res = b.bot.Prefix(prefix)
	if res == nil {
		res = b.bgr.Prefix(prefix)
	}

	if res == nil {
		res = b.ldb.Prefix(prefix)
	}
	return
}

func (b *Ha) Suffix(suffix []byte) (res [][]byte) {
	res = b.bot.Suffix(suffix)
	if res == nil {
		res = b.bgr.Suffix(suffix)
	}
	if res == nil {
		res = b.ldb.Suffix(suffix)
	}
	return
}

func (b *Ha) Scan() (res [][]byte) {
	res = b.bot.Scan()
	if res == nil {
		res = b.bgr.Scan()
	}
	if res == nil {
		res = b.ldb.Scan()
	}
	return
}

func (b *Ha) SetTTL(k, v []byte, expire time.Duration) (err error) {
	go b.bot.SetTTL(k, v, expire)
	go b.bgr.SetTTL(k, v, expire)
	return b.ldb.SetTTL(k, v, expire)
}

func (b *Ha) Range(start, limit []byte) (res [][]byte) {
	res = b.bot.Range(start, limit)
	if res == nil {
		res = b.bgr.Range(start, limit)
	}

	if res == nil {
		res = b.ldb.Range(start, limit)
	}

	return
}

func (b *Ha) Close() error {
	b.bot.Close()
	b.ldb.Close()
	b.bgr.Close()
	return nil
}
