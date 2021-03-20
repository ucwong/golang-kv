package badger

import (
	"bytes"
	badger "github.com/dgraph-io/badger/v3"
)

type Badger struct {
	engine *badger.DB
}

func New() *Badger {
	b := &Badger{}
	if bg, err := badger.Open(badger.DefaultOptions(".badger")); err == nil {
		b.engine = bg
	} else {
		panic("badger open failed")
	}
	return b
}

func (b *Badger) Get(k []byte) (v []byte) {
	b.engine.View(func(txn *badger.Txn) error {
		if item, err := txn.Get(k); err == nil {
			if val, err := item.ValueCopy(nil); err == nil {
				v = val
			}
		}
		return nil
	})
	return
}

func (b *Badger) Set(k, v []byte) (err error) {
	err = b.engine.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(k), []byte(v))
	})
	return
}

func (b *Badger) Del(k []byte) error {
	return nil
}

func (b *Badger) Prefix(k []byte) (res [][]byte) {
	b.engine.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(k); it.ValidForPrefix(k); it.Next() {
			item := it.Item()
			item.Value(func(v []byte) error {
				res = append(res, v)
				return nil
			})
		}
		return nil
	})
	return
}

func (b *Badger) Suffix(k []byte) (res [][]byte) {
	b.engine.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if bytes.HasSuffix(item.Key(), k) {
				item.Value(func(v []byte) error {
					res = append(res, v)
					return nil
				})
			}
		}
		return nil
	})
	return
}

func (b *Badger) Scan() (res [][]byte) {
	return nil
}

func (b *Badger) Close() error {
	b.engine.Close()
	return nil
}
