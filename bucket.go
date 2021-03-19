package bucket

import (
	"github.com/ucwong/bucket/badger"
)

func New() Bucket {
	return badger.New()
}
