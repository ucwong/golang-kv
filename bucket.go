package bucket

import (
	"github.com/ucwong/bucket/badger"
	"github.com/ucwong/bucket/bolt"
	"github.com/ucwong/bucket/leveldb"
)

func Badger(path string) Bucket {
	return badger.Open(path)
}

func Bolt(path string) Bucket {
	return bolt.Open(path)
}

func LevelDB(path string) Bucket {
	return leveldb.Open(path)
}
