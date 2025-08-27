package flatfs_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
)

func TestBatchWritesToTempUntilCommit(t *testing.T) {
	tryAllShardFuncs(t, testBatchWritesToTempUntilCommit)
}

func testBatchWritesToTempUntilCommit(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	// Create a batch
	batch, err := fs.Batch(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Put some keys
	keys := []string{"QUUX", "QAAX", "QBBX"}
	for _, key := range keys {
		err = batch.Put(context.Background(), datastore.NewKey(key), []byte("testdata"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check that files don't exist in the main datastore yet
	for _, key := range keys {
		has, err := fs.Has(context.Background(), datastore.NewKey(key))
		if err != nil {
			t.Fatal(err)
		}
		if has {
			t.Errorf("key %s should not exist in datastore before commit", key)
		}
	}

	// Check that no data files exist in shard directories
	checkNoDataFiles := func() bool {
		found := false
		filepath.Walk(temp, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if filepath.Ext(path) == ".data" {
				relPath, _ := filepath.Rel(temp, path)
				// Ignore if it's in .temp directory
				if !isInTempDir(relPath) {
					t.Errorf("found data file before commit: %s", relPath)
					found = true
				}
			}
			return nil
		})
		return found
	}

	if checkNoDataFiles() {
		t.Fatal("data files found in main directories before commit")
	}

	// Now commit
	err = batch.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// After commit, all keys should exist
	for _, key := range keys {
		has, err := fs.Has(context.Background(), datastore.NewKey(key))
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Errorf("key %s should exist in datastore after commit", key)
		}
	}
}

func isInTempDir(path string) bool {
	// Check if path starts with .temp/ or contains /.temp/
	return len(path) >= 6 && (path[:6] == ".temp/" || path[:6] == ".temp\\")
}