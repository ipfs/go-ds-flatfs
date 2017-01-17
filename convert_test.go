package flatfs_test

import (
	"bytes"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-flatfs"
	//"gx/ipfs/QmRWDav6mzWseLWeYfVd5fvUKiVe9xNH29YfMF438fG364/go-datastore/query"

	rand "github.com/dustin/randbo"
)

func TestMove(t *testing.T) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	v1dir := filepath.Join(tempdir, "v1")
	err := flatfs.Create(v1dir, flatfs.Prefix(3))
	if err != nil {
		t.Fatalf("Create fail: %v\n", err)
	}
	err = ioutil.WriteFile(filepath.Join(v1dir, "README_ALSO"), []byte("something"), 0666)
	if err != nil {
		t.Fatalf("WriteFile fail: %v\n", err)
	}
	v1, err := flatfs.Open(v1dir, false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	r := rand.New()
	var blocks [][]byte
	var keys []datastore.Key
	for i := 0; i < 256; i++ {
		blk := make([]byte, 1000)
		r.Read(blk)
		blocks = append(blocks, blk)

		key := "x" + hex.EncodeToString(blk[:8])
		keys = append(keys, datastore.NewKey(key))
		err := v1.Put(keys[i], blocks[i])
		if err != nil {
			t.Fatalf("Put fail: %v\n", err)
		}
	}

	v2dir := filepath.Join(tempdir, "v2")
	err = flatfs.Create(v2dir, flatfs.NextToLast(2))
	if err != nil {
		t.Fatalf("Mkdir fail: %v\n", err)
	}
	flatfs.Move(v1dir, v2dir, nil)

	// This should fail if the directory is not empty
	err = os.Remove(v1dir)
	if err != nil {
		t.Fatalf("Remove fail: %v\n", err)
	}

	// Make sure the README file moved
	_, err = os.Stat(filepath.Join(v2dir, "README_ALSO"))
	if err != nil {
		t.Fatalf(err.Error())
	}

	v2, err := flatfs.Open(v2dir, false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	// Sanity check, make sure we can retrieve a new key
	data, err := v2.Get(keys[0])
	if err != nil {
		t.Fatalf("Get fail: %v\n", err)
	}
	if !bytes.Equal(data.([]byte), blocks[0]) {
		t.Fatalf("block context differ for key %s\n", keys[0].String())
	}

	shard := filepath.Join(v2dir, flatfs.NextToLast(2).Func()(keys[0].String()))
	_, err = os.Stat(shard)
	if err != nil {
		t.Fatalf(err.Error())
	}
}
