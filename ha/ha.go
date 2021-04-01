package ha

import (
	"github.com/ucwong/golang-kv/badger"
	"github.com/ucwong/golang-kv/bolt"
	"github.com/ucwong/golang-kv/leveldb"
	"sync"
	"time"
)

type Ha struct {
	ldb *leveldb.LevelDB
	bgr *badger.Badger
	bot *bolt.Bolt
	wg  sync.WaitGroup
}

func Open(path string) *Ha {
	if len(path) == 0 {
		path = ".ha"
	}

	ha := &Ha{}
	ha.bot = bolt.Open(path + ".bolt")
	ha.bgr = badger.Open(path + ".badger")
	ha.ldb = leveldb.Open(path + ".leveldb")

	if ha.bot == nil || ha.bgr == nil || ha.ldb == nil {
		return nil
	}

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
	b.wg.Add(3)
	go func() {
		defer b.wg.Done()
		b.bot.Set(k, v)
	}()
	go func() {
		defer b.wg.Done()
		b.bgr.Set(k, v)
	}()
	go func() {
		defer b.wg.Done()
		b.ldb.Set(k, v)
	}()
	b.wg.Wait()
	return
}

func (b *Ha) Del(k []byte) (err error) {
	b.wg.Add(3)
	go func() {
		defer b.wg.Done()
		b.bot.Del(k)
	}()
	go func() {
		defer b.wg.Done()
		b.bgr.Del(k)
	}()
	go func() {
		defer b.wg.Done()
		b.ldb.Del(k)
	}()

	b.wg.Wait()

	return
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
	b.wg.Add(3)

	go func() {
		defer b.wg.Done()
		b.bot.SetTTL(k, v, expire)
	}()

	go func() {
		defer b.wg.Done()
		b.bgr.SetTTL(k, v, expire)
	}()

	go func() {
		defer b.wg.Done()
		b.ldb.SetTTL(k, v, expire)
	}()

	b.wg.Wait()

	return
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

func (b *Ha) Close() (err error) {
	b.wg.Add(3)

	go func() {
		defer b.wg.Done()
		b.bot.Close()
	}()

	go func() {
		defer b.wg.Done()
		b.ldb.Close()
	}()

	go func() {
		defer b.wg.Done()
		b.bgr.Close()
	}()

	b.wg.Wait()

	return
}
