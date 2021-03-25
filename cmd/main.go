package main

import (
	"fmt"
	"github.com/ucwong/bucket"
	"strconv"
	"time"
)

func main() {
	bolt()
	badger()
	leveldb()
}

var batch int = 2

func leveldb() {
	var db bucket.Bucket

	db = bucket.LevelDB(".leveldb")

	db.Set([]byte("yx"), []byte("yx"))
	db.Set([]byte("yy"), []byte("yy"))
	db.Set([]byte("aabb"), []byte("aabb"))
	db.Set([]byte("bb"), []byte("bb"))
	db.Set([]byte("x"), []byte("x"))
	db.Set([]byte("y"), []byte("y"))
	db.Set([]byte("xxy"), []byte("xxy"))
	db.Set([]byte("xxxyx"), []byte("xxxyx"))

	db.SetTTL([]byte("ttlxxxyx"), []byte("ttlxxxyx"), 1000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx1"), []byte("ttlxxxyx1"), 2000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx2"), []byte("ttlxxxyx2"), 5000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx3"), []byte("ttlxxxyx3"), 5000*time.Millisecond)
	for i := 0; i < batch; i++ {
		db.SetTTL([]byte("ttlxxxyx3"+strconv.Itoa(i)), []byte("ttlxxxyx3"+strconv.Itoa(i)), 2000*time.Millisecond)
	}
	for i := 0; i < batch; i++ {
		db.SetTTL([]byte("ttlxxxyx4"+strconv.Itoa(i)), []byte("ttlxxxyx4"+strconv.Itoa(i)), 5000*time.Millisecond)
	}
	res := db.Scan()
	for _, i := range res {
		fmt.Printf("scan...%v...%s\n", len(res), string(i))
	}
	res = db.Prefix([]byte("xx"))
	for _, i := range res {
		fmt.Printf("prefix(xx)...%v...%s\n", len(res), string(i))
	}
	res = db.Suffix([]byte("x"))
	for _, i := range res {
		fmt.Printf("suffix(x)...%v...%s\n", len(res), string(i))
	}
	res = db.Scan()
	for _, i := range res {
		fmt.Printf("scan...%v...%s\n", len(res), string(i))
	}
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
		fmt.Printf("..........%s    .%s\n", "ttlxxxyx4"+strconv.Itoa(i), string(db.Get([]byte("ttlxxxyx4"+strconv.Itoa(i)))))
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

func bolt() {
	var db bucket.Bucket

	db = bucket.Bolt(".bolt")
	db.Set([]byte("a"), []byte("a"))
	db.Set([]byte("b"), []byte("b"))
	db.Set([]byte("x"), []byte("x"))
	db.Set([]byte("xxy"), []byte("xxy"))
	db.Set([]byte("xxxyx"), []byte("xxxyx"))
	db.SetTTL([]byte("ttlxxxyx"), []byte("ttlxxxyx"), 1000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx1"), []byte("ttlxxxyx1"), 2000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx2"), []byte("ttlxxxyx2"), 5000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx3"), []byte("ttlxxxyx3"), 5000*time.Millisecond)
	for i := 0; i < batch; i++ {
		db.SetTTL([]byte("ttlxxxyx3"+strconv.Itoa(i)), []byte("ttlxxxyx3"+strconv.Itoa(i)), 2000*time.Millisecond)
	}
	for i := 0; i < batch; i++ {
		db.SetTTL([]byte("ttlxxxyx4"+strconv.Itoa(i)), []byte("ttlxxxyx4"+strconv.Itoa(i)), 5000*time.Millisecond)
	}
	res := db.Scan()
	for _, i := range res {
		fmt.Printf("scan...%v...%s\n", len(res), string(i))
	}
	res = db.Prefix([]byte("xx"))
	for _, i := range res {
		fmt.Printf("prefix...%v...%s\n", len(res), string(i))
	}
	res = db.Suffix([]byte("x"))
	for _, i := range res {
		fmt.Printf("suffix...%v...%s\n", len(res), string(i))
	}
	res = db.Scan()
	for _, i := range res {
		fmt.Printf("scan...%v...%v\n", len(res), len(i))
	}
	//db.Del([]byte("xxy"))
	//res = db.Scan()
	//for _, i := range res {
	//      fmt.Printf("...%v...%s\n", len(res), string(i))
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

func badger() {
	var db bucket.Bucket

	db = bucket.Badger(".badger")
	db.Set([]byte("y"), []byte("y"))
	db.Set([]byte("x"), []byte("x"))
	db.Set([]byte("xxy"), []byte("xxy"))
	db.Set([]byte("xxxyx"), []byte("xxxyx"))
	db.SetTTL([]byte("ttlxxxyx"), []byte("ttlxxxyx"), 1000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx1"), []byte("ttlxxxyx1"), 2000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx2"), []byte("ttlxxxyx2"), 5000*time.Millisecond)
	db.SetTTL([]byte("ttlxxxyx3"), []byte("ttlxxxyx3"), 5000*time.Millisecond)
	for i := 0; i < batch; i++ {
		db.SetTTL([]byte("ttlxxxyx3"+strconv.Itoa(i)), []byte("ttlxxxyx3"+strconv.Itoa(i)), 2000*time.Millisecond)
	}
	for i := 0; i < batch; i++ {
		db.SetTTL([]byte("ttlxxxyx4"+strconv.Itoa(i)), []byte("ttlxxxyx4"+strconv.Itoa(i)), 2000*time.Millisecond)
	}
	res := db.Scan()
	for _, i := range res {
		fmt.Printf("scan...%v...%s\n", len(res), string(i))
	}
	res = db.Prefix([]byte("x"))
	for _, i := range res {
		fmt.Printf("prefix...%v...%s\n", len(res), string(i))
	}
	res = db.Suffix([]byte("x"))
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
	//      fmt.Printf("...%v...%s\n", len(res), string(i))
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
