package bolt

import (
	"bytes"
	bolt "go.etcd.io/bbolt"
)

type Bolt struct {
	engine *bolt.DB
}

const GLOBAL = "bolt"

func New() *Bolt {
	b := &Bolt{}
	if db, err := bolt.Open(".bolt", 0600, nil); err == nil {
		b.engine = db
	} else {
		panic("bolt open failed")
	}
	return b
}

func (b *Bolt) Get(k []byte) (v []byte) {
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
		for k, v := c.Seek(suffix); k != nil && bytes.HasSuffix(k, suffix); k, v = c.Next() {
			res = append(res, v)
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

func (b *Bolt) Close() error {
	b.engine.Close()
	return nil
}
