package bolt

import (
	"bytes"
	//"errors"
	"fmt"
	"time"

	"github.com/imkira/go-ttlmap"
	bolt "go.etcd.io/bbolt"
)

type Bolt struct {
	engine  *bolt.DB
	ttl_map *ttlmap.Map
}

const GLOBAL = "bolt"

func New() *Bolt {
	b := &Bolt{}
	if db, err := bolt.Open(".bolt", 0600, nil); err == nil {
		b.engine = db
	} else {
		panic("bolt open failed")
	}

	options := &ttlmap.Options{
		InitialCapacity: 1024,
		OnWillExpire: func(key string, item ttlmap.Item) {
			fmt.Printf("expired: [%s=%v]\n", key, item.Value())
			//b.Del([]byte(key))
		},
		OnWillEvict: func(key string, item ttlmap.Item) {
			fmt.Printf("evicted: [%s=%v]\n", key, item.Value())
			b.Del([]byte(key))
		},
	}
	b.ttl_map = ttlmap.New(options)
	return b
}

func (b *Bolt) Get(k []byte) (v []byte) {
	item, err := b.ttl_map.Get(string(k))
	if err == nil {
		return []byte(item.Value().(string))
	}
	b.engine.View(func(tx *bolt.Tx) error {
		buk := tx.Bucket([]byte(GLOBAL))
		if buk != nil {
			v = buk.Get(k)
		}
		return nil
	})
	return
}

func (b *Bolt) Set(k, v []byte) (err error) {
	b.ttl_map.Delete(string(k))
	err = b.engine.Update(func(tx *bolt.Tx) error {
		buk, e := tx.CreateBucketIfNotExists([]byte(GLOBAL))
		if e != nil {
			return e
		}
		return buk.Put(k, v)
	})
	return
}

func (b *Bolt) Del(k []byte) (err error) {
	err = b.engine.Update(func(tx *bolt.Tx) error {
		buk := tx.Bucket([]byte(GLOBAL))
		if buk == nil {
			return nil
		}
		return buk.Delete(k)
	})

	return
}

func (b *Bolt) Prefix(prefix []byte) (res [][]byte) {
	b.engine.View(func(tx *bolt.Tx) error {
		buk := tx.Bucket([]byte(GLOBAL))
		if buk == nil {
			return nil
		}
		c := buk.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			res = append(res, v)
		}

		return nil
	})

	return
}

func (b *Bolt) Suffix(suffix []byte) (res [][]byte) {
	b.engine.View(func(tx *bolt.Tx) error {
		buk := tx.Bucket([]byte(GLOBAL))
		if buk == nil {
			return nil
		}

		c := buk.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if bytes.HasSuffix(k, suffix) {
				res = append(res, v)
			}
		}

		return nil
	})

	return
}

func (b *Bolt) Scan() (res [][]byte) {
	b.engine.View(func(tx *bolt.Tx) error {
		buk := tx.Bucket([]byte(GLOBAL))
		if buk == nil {
			return nil
		}
		buk.ForEach(func(k, v []byte) error {
			res = append(res, v)
			return nil
		})
		return nil
	})

	return
}

func (b *Bolt) SetTTL(k, v []byte, expire time.Duration) (err error) {
	go b.ttl_map.Set(string(k), ttlmap.NewItem(string(v), ttlmap.WithTTL(expire)), nil)
	return b.Set(k, v)
}

func (b *Bolt) Close() error {
	go b.ttl_map.Drain()
	return b.engine.Close()
}
