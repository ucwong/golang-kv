package bucket

import (
	"github.com/ucwong/bucket/badger"
	"github.com/ucwong/bucket/bolt"
)

func Badger() Bucket {
	return badger.New()
}

func Bolt() Bucket {
	return bolt.New()
}
