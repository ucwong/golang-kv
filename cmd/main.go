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
	db.Close()

	db = bucket.New()
	db.Set([]byte("x"), []byte("y"))
	v = db.Get([]byte("x"))
	fmt.Println("..." + string(v))
	//db.Close()

	db.Set([]byte("xxy"), []byte("xxy"))
	db.Set([]byte("xxxy"), []byte("xxxy"))
	res := db.Suffix([]byte("xy"))
	for _, i := range res {
		fmt.Printf("...%v...%s\n", len(res), string(i))
	}
	db.Del([]byte("xx"))
	db.Close()
}
