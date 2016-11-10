package main

import (
	"io/ioutil"
	"os"
	"math/rand"
	"time"
	"fmt"
	"math"
	//"path/filepath"
	//"strings"
	
	ds "gx/ipfs/QmbzuUusHqaLLoNTDEVLcSF6vZDHZDLPC7p4bztRvvkXxU/go-datastore"
	dsq "gx/ipfs/QmbzuUusHqaLLoNTDEVLcSF6vZDHZDLPC7p4bztRvvkXxU/go-datastore/query"

	db "github.com/ipfs/go-ds-flatfs"
)

func main() {
	TimeQuery()
}

var N = 10000
var runs = 100

func TimeQuery() {
	rand.Seed(time.Now().UTC().UnixNano())
	path, err := ioutil.TempDir("/tmp", "flatdb_")
	if err != nil {
		panic(err)
	}

	d, err := db.New(path, 5, false)
	if err != nil {
		panic(err)
	}
	defer func() {
		os.RemoveAll(path)
		d.Close()
	}()
	for n := 0; n < N; n++ {
		err := d.Put(ds.NewKey(RandomString(52)), []byte(RandomString(1000)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i <= runs; i++ {
		which := rand.Intn(2)
		switch which {
		case 0:
			TestQuery(d)
		case 1:
			TestQueryOpt(d)
		default:
			panic("!")
		}
	}
	queryRes.report("ORIG")
	queryOptRes.report("OPT")
}

var queryRes res
var queryOptRes res

func TestQuery(d *db.Datastore) {
	start := time.Now()
	rs, err := d.Query(dsq.Query{KeysOnly: true})
	if err != nil {
		panic(err)
	}
	i := 0
	for r := range rs.Next() {
		i += int(r.Key[1])
	}
	elapsed := time.Since(start)
	queryRes.add(elapsed.Seconds()*1000)
	fmt.Printf("%d orig %f %d\n", N, elapsed.Seconds()*1000, i)
}

func TestQueryOpt(d *db.Datastore) {
	start := time.Now()
	rs, err := d.QueryOpt(dsq.Query{KeysOnly: true})
	if err != nil {
		panic(err)
	}
	i := 0
	for r := range rs.Next() {
		i += int(r.Key[1])
	}
	elapsed := time.Since(start)
	queryOptRes.add(elapsed.Seconds()*1000)
	fmt.Printf("%d opt %f %d\n", N, elapsed.Seconds()*1000, i)
}

type res struct {
	num   float64
	sum   float64
	sumSq float64
} 

func (r *res) add(v float64) {
	r.num += 1.0
	r.sum += v
	r.sumSq += v*v
}

func (r * res) report(what string) {
	mean := r.sum/r.num
	stddev := math.Sqrt(r.sumSq/r.num - mean*mean)
	fmt.Printf("%d %s %f (+/- %f) ms\n", N, what, mean, stddev)
}

func RandomString(strlen int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

