package main

import (
	"fmt"
	"github.com/ucwong/bucket"
	"strconv"
	"time"
)

func main() {
	var batch int = 10
	var db bucket.Bucket

	db = bucket.Bolt()
	res := db.Scan()
	for _, i := range res {
		fmt.Printf("scan...%v...%s\n", len(res), string(i))
	}
	db.Set([]byte("x"), []byte("y"))
	db.Set([]byte("xxy"), []byte("xxy"))
	db.Set([]byte("xxxyx"), []byte("xxxyx"))
	db.SetTTL([]byte("ttlxxxyx"), []byte("ttlxxxyx"), 1000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx1"), []byte("ttlxxxyx1"), 2000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx2"), []byte("ttlxxxyx2"), 5000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx3"), []byte("ttlxxxyx3"), 5000*time.Millisecond)
	for i := 0; i < batch; i++ {
		db.SetTTL([]byte("ttlxxxyx4"+strconv.Itoa(i)), []byte("ttlxxxyx4"+strconv.Itoa(i)), 5000*time.Millisecond)
	}
	res = db.Prefix([]byte("xx"))
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

	for i := 0; i < batch/2; i++ {
		db.Set([]byte("ttlxxxyx4"+strconv.Itoa(i)), []byte("reset -> ttlxxxyx4"+strconv.Itoa(i)))
	}

	for i := 0; i < batch; i++ {
		fmt.Printf("...........%s\n", string(db.Get([]byte("ttlxxxyx4"+strconv.Itoa(i)))))
	}

	db.Del([]byte("ttlxxxyx1"))

	time.Sleep(3000 * time.Millisecond)
	m := db.Get([]byte("ttlxxxyx"))
	fmt.Printf("...........%s\n", string(m))

	db.Del([]byte("ttlxxxyx1"))

	m2 := db.Get([]byte("ttlxxxyx1"))
	fmt.Printf("...........%s\n", string(m2))

	f2 := db.Get([]byte("xxy"))
	fmt.Printf("...........%s\n", string(f2))
	db.Close()
}
