// Copyright (C) 2022 ucwong
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>

package bolt

import (
	"fmt"
	"strconv"

	//"github.com/ucwong/golang-kv"
	"testing"
	"time"
)

func TestLocal(t *testing.T) {
	bolt1()
}

var batch int = 10

func bolt1() {

	db := Open("")
	if db == nil {
		panic("bolt create err")
	}
	db.Set([]byte("yx"), []byte("yx"))
	db.Set([]byte("yy"), []byte("yy"))
	db.Set([]byte("a"), []byte("a"))
	db.Set([]byte("b"), []byte("b"))
	db.Set([]byte("x"), []byte("x"))
	db.Set([]byte("y"), []byte("y"))
	db.Set([]byte("xxy"), []byte("xxy"))
	db.Set([]byte("xxx"), []byte("xxx"))
	db.Set([]byte("xxxyx"), []byte("xxxyx"))
	db.Set([]byte("xyy"), []byte("xyy"))
	db.SetTTL([]byte("ttlxxxyx"), []byte("ttlxxxyx"), 1000*time.Millisecond)
	db.Set([]byte("ttlxxxyx"), []byte("ttlxxxyx"))
	db.SetTTL([]byte("ttlxxxyx1"), []byte("ttlxxxyx1"), 200000*time.Millisecond)
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
	res = db.Range([]byte("xxx"), []byte("xxz"))
	for _, i := range res {
		fmt.Printf("range...%v...%s\n", len(res), string(i))
	}
	res = db.Prefix([]byte("xx"))
	for _, i := range res {
		fmt.Printf("prefix...%v...%s\n", len(res), string(i))
	}
	res = db.Suffix([]byte("x"))
	for _, i := range res {
		fmt.Printf("suffix...%v...%s\n", len(res), string(i))
	}
	kvs := make(map[string][]byte)
	kvs["batch1"] = []byte("batchv1")
	kvs["batch2"] = []byte("batchv2")
	kvs["batch3"] = []byte("batchv3")
	kvs["batch4"] = []byte("batchv4")
	db.BatchSet(kvs)
	res = db.Prefix([]byte("batch"))
	for _, i := range res {
		fmt.Printf("prefix(batch)...%v...%s\n", len(res), string(i))
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
	fmt.Printf("Close func call\n")
	db.Close()
}
