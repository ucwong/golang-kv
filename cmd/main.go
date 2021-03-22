package main

import (
	"fmt"
	"github.com/ucwong/bucket"
	"time"
)

func main() {
	var db bucket.Bucket

	db = bucket.Bolt()
	db.Set([]byte("x"), []byte("y"))
	db.Set([]byte("xxy"), []byte("xxy"))
	db.Set([]byte("xxxyx"), []byte("xxxyx"))
	db.SetTTL([]byte("ttlxxxyx"), []byte("ttlxxxyx"), 1000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx1"), []byte("ttlxxxyx1"), 2000*time.Millisecond)
	res := db.Prefix([]byte("xx"))
	for _, i := range res {
		fmt.Printf("prefix...%v...%s\n", len(res), string(i))
	}
	res = db.Suffix([]byte("xxyx"))
	for _, i := range res {
		fmt.Printf("suffix...%v...%s\n", len(res), string(i))
	}
	res = db.Scan()
	for _, i := range res {
		fmt.Printf("scan...%v...%s\n", len(res), string(i))
	}
	//db.Del([]byte("xxy"))
	//res = db.Scan()
	//for _, i := range res {
	//	fmt.Printf("...%v...%s\n", len(res), string(i))
	//}
	db.Del([]byte("xx"))
	time.Sleep(500 * time.Millisecond)
	f := db.Get([]byte("ttlxxxyx"))
	fmt.Printf("...........%s\n", string(f))

	f1 := db.Get([]byte("xxy"))
	fmt.Printf("...........%s\n", string(f1))

	db.Set([]byte("ttlxxxyx"), []byte("ttl reset"))

	time.Sleep(3000 * time.Millisecond)
	m := db.Get([]byte("ttlxxxyx"))
	fmt.Printf("...........%s\n", string(m))

	m2 := db.Get([]byte("ttlxxxyx1"))
	fmt.Printf("...........%s\n", string(m2))

	f2 := db.Get([]byte("xxy"))
	fmt.Printf("...........%s\n", string(f2))
	db.Close()
}
