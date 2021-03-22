package badger

import (
	"bytes"
	"time"

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

func (b *Badger) Del(k []byte) (err error) {
	err = b.engine.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
	return
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
	b.engine.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				res = append(res, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return
}

func (b *Badger) SetTTL(k, v []byte, expire time.Duration) (err error) {
	err = b.engine.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(k, v).WithTTL(expire)
		return txn.SetEntry(e)
	})
	return
}

func (b *Badger) Close() error {
	return b.engine.Close()
}
