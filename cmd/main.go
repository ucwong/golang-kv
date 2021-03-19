package main

import (
	"fmt"
	"github.com/ucwong/bucket"
)

func main() {
	var db bucket.Bucket
	db = bucket.Bolt()
	db.Set([]byte("x"), []byte("y"))
	v := db.Get([]byte("x"))
	fmt.Println("..." + string(v))
	db.Del([]byte("x"))
	db.Close()

	db = bucket.Badger()
	db.Set([]byte("x"), []byte("y"))
	v = db.Get([]byte("x"))
	fmt.Println("..." + string(v))
	db.Del([]byte("x"))
	db.Close()
}
