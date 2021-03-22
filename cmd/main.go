package main

import (
	"fmt"
	"github.com/ucwong/bucket"
)

func main() {
	var db bucket.Bucket

	db = bucket.Bolt()
	db.Set([]byte("x"), []byte("y"))
	db.Set([]byte("xxy"), []byte("xxy"))
	db.Set([]byte("xxxy"), []byte("xxxy"))
	res := db.Prefix([]byte("xxy"))
	for _, i := range res {
		fmt.Printf("...%v...%s\n", len(res), string(i))
	}
	res = db.Suffix([]byte("xxy"))
	for _, i := range res {
		fmt.Printf("...%v...%s\n", len(res), string(i))
	}
	res = db.Scan()
	for _, i := range res {
		fmt.Printf("...%v...%s\n", len(res), string(i))
	}
	//db.Del([]byte("xxy"))
	//res = db.Scan()
	//for _, i := range res {
	//	fmt.Printf("...%v...%s\n", len(res), string(i))
	//}
	db.Del([]byte("xx"))
	db.Close()
}
