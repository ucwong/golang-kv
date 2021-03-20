package bolt

import (
	bolt "go.etcd.io/bbolt"
)

type Bolt struct {
	engine *bolt.DB
}

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
		buk := tx.Bucket([]byte("bolt"))
		if buk != nil {
			v = buk.Get(k)
		}
		return nil
	})
	return
}

func (b *Bolt) Set(k, v []byte) (err error) {
	b.engine.Update(func(tx *bolt.Tx) error {
		buk, err := tx.CreateBucketIfNotExists([]byte("bolt"))
		if err != nil {
			return err
		}
		err = buk.Put(k, v)
		return err
	})
	return
}

func (b *Bolt) Del(k []byte) error {
	return nil
}

func (b *Bolt) Prefix(k []byte) (res [][]byte) {
	return nil
}

func (b *Bolt) Suffix(k []byte) (res [][]byte) {
	return nil
}

func (b *Bolt) Scan() (res [][]byte) {
	return nil
}

func (b *Bolt) Close() error {
	b.engine.Close()
	return nil
}
