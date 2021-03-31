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
	ha.ldb = leveldb.Open(path + ".leveldb")
	ha.bgr = badger.Open(path + ".badger")
	ha.bot = bolt.Open(path + ".bot")

	return ha
}

func (b *Ha) Get(k []byte) (v []byte) {
	v = b.bot.Get(k)
	if v == nil {
		v = b.bgr.Get(k)
	}
	return
}

func (b *Ha) Set(k, v []byte) (err error) {
	go b.bot.Set(k, v)
	go b.ldb.Set(k, v)
	return b.bgr.Set(k, v)
}

func (b *Ha) Del(k []byte) (err error) {
	go b.bot.Del(k)
	go b.ldb.Del(k)
	return b.bgr.Del(k)
}

func (b *Ha) Prefix(prefix []byte) (res [][]byte) {
	res = b.bot.Prefix(prefix)
	if res == nil {
		res = b.bgr.Prefix(prefix)
	}
	return
}

func (b *Ha) Suffix(suffix []byte) (res [][]byte) {
	res = b.bot.Suffix(suffix)
	if res == nil {
		res = b.bgr.Suffix(suffix)
	}
	return
}

func (b *Ha) Scan() (res [][]byte) {
	res = b.bot.Scan()
	if res == nil {
		res = b.bgr.Scan()
	}
	return
}

func (b *Ha) SetTTL(k, v []byte, expire time.Duration) (err error) {
	go b.bot.SetTTL(k, v, expire)
	go b.ldb.SetTTL(k, v, expire)
	return b.bgr.SetTTL(k, v, expire)
}

func (b *Ha) Range(start, limit []byte) (res [][]byte) {
	res = b.bot.Range(start, limit)
	if res == nil {
		res = b.bgr.Range(start, limit)
	}

	return
}

func (b *Ha) Close() error {
	b.ldb.Close()
	b.bgr.Close()
	b.bot.Close()
	return nil
}
