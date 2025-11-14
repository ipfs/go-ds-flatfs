package flatfs_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
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

func TestBatchReadOperations(t *testing.T) {
	tryAllShardFuncs(t, testBatchReadOperations)
}

func testBatchReadOperations(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	// Put some initial data in the datastore
	initialKey := datastore.NewKey("INITIAL")
	initialData := []byte("initial data")
	err = fs.Put(context.Background(), initialKey, initialData)
	if err != nil {
		t.Fatal(err)
	}

	// Create a batch
	batch, err := fs.Batch(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Cast to BatchReader interface
	batchReader, ok := batch.(flatfs.BatchReader)
	if !ok {
		t.Fatal("batch does not implement BatchReader interface")
	}

	// Put a new key in batch
	batchKey := datastore.NewKey("BATCH")
	batchData := []byte("batch data")
	err = batch.Put(context.Background(), batchKey, batchData)
	if err != nil {
		t.Fatal(err)
	}

	// Overwrite initial key in batch
	overwriteData := []byte("overwritten data")
	err = batch.Put(context.Background(), initialKey, overwriteData)
	if err != nil {
		t.Fatal(err)
	}

	// Delete a key that will be created
	deleteKey := datastore.NewKey("TODELETE")
	err = fs.Put(context.Background(), deleteKey, []byte("to be deleted"))
	if err != nil {
		t.Fatal(err)
	}
	err = batch.Delete(context.Background(), deleteKey)
	if err != nil {
		t.Fatal(err)
	}

	// Test Get operations before commit
	// 1. Get from batch (new key)
	data, err := batchReader.Get(context.Background(), batchKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(batchData) {
		t.Errorf("expected %s, got %s", batchData, data)
	}

	// 2. Get overwritten key should return new data from batch
	data, err = batchReader.Get(context.Background(), initialKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(overwriteData) {
		t.Errorf("expected %s, got %s", overwriteData, data)
	}

	// 3. Get deleted key should return not found
	_, err = batchReader.Get(context.Background(), deleteKey)
	if err != datastore.ErrNotFound {
		t.Errorf("expected ErrNotFound for deleted key, got %v", err)
	}

	// Test Has operations before commit
	// 1. Has for new key in batch
	has, err := batchReader.Has(context.Background(), batchKey)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Error("expected batch key to exist")
	}

	// 2. Has for overwritten key
	has, err = batchReader.Has(context.Background(), initialKey)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Error("expected initial key to exist")
	}

	// 3. Has for deleted key should return false
	has, err = batchReader.Has(context.Background(), deleteKey)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Error("expected deleted key to not exist")
	}

	// Test GetSize operations before commit
	size, err := batchReader.GetSize(context.Background(), batchKey)
	if err != nil {
		t.Fatal(err)
	}
	if size != len(batchData) {
		t.Errorf("expected size %d, got %d", len(batchData), size)
	}

	// GetSize for deleted key should return not found
	_, err = batchReader.GetSize(context.Background(), deleteKey)
	if err != datastore.ErrNotFound {
		t.Errorf("expected ErrNotFound for deleted key size, got %v", err)
	}

	// Main datastore should still have original data
	data, err = fs.Get(context.Background(), initialKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(initialData) {
		t.Errorf("main datastore should still have original data: expected %s, got %s", initialData, data)
	}

	// Commit the batch
	err = batch.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Verify final state in main datastore
	// 1. New key should exist
	data, err = fs.Get(context.Background(), batchKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(batchData) {
		t.Errorf("expected %s, got %s", batchData, data)
	}

	// 2. Initial key should be overwritten
	data, err = fs.Get(context.Background(), initialKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(overwriteData) {
		t.Errorf("expected %s, got %s", overwriteData, data)
	}

	// 3. Deleted key should not exist
	_, err = fs.Get(context.Background(), deleteKey)
	if err != datastore.ErrNotFound {
		t.Errorf("expected ErrNotFound for deleted key after commit, got %v", err)
	}
}

func TestBatchDiscard(t *testing.T) {
	tryAllShardFuncs(t, testBatchDiscard)
}

func testBatchDiscard(dirFunc mkShardFunc, t *testing.T) {
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

	// Cast to DiscardableBatch interface
	discardable, ok := batch.(flatfs.DiscardableBatch)
	if !ok {
		t.Fatal("batch does not implement DiscardableBatch interface")
	}

	// Discard the batch
	err = discardable.Discard(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Check that files still don't exist in the main datastore
	for _, key := range keys {
		has, err := fs.Has(context.Background(), datastore.NewKey(key))
		if err != nil {
			t.Fatal(err)
		}
		if has {
			t.Errorf("key %s should not exist in datastore after discard", key)
		}
	}

	// Verify temp directory was cleaned up
	tempBatchDirs, err := filepath.Glob(filepath.Join(temp, ".temp", "batch-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tempBatchDirs) > 0 {
		t.Errorf("batch temp directories should be cleaned up after discard, found: %v", tempBatchDirs)
	}
}

func TestBatchQuery(t *testing.T) {
	tryAllShardFuncs(t, testBatchQuery)
}

func testBatchQuery(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("CreateOrOpen fail: %v\n", err)
	}
	defer fs.Close()

	ctx := context.Background()

	// Add some data to the main datastore
	mainKeys := []string{"EXISTING1", "EXISTING2", "EXISTING3"}
	for _, k := range mainKeys {
		err := fs.Put(ctx, datastore.NewKey(k), []byte("main:"+k))
		if err != nil {
			t.Fatalf("Put fail: %v\n", err)
		}
	}

	// Create a batch
	batch, err := fs.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Add new keys to batch
	batchKeys := []string{"BATCH1", "BATCH2"}
	for _, k := range batchKeys {
		err := batch.Put(ctx, datastore.NewKey(k), []byte("batch:"+k))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Delete one existing key
	err = batch.Delete(ctx, datastore.NewKey("EXISTING2"))
	if err != nil {
		t.Fatal(err)
	}

	// Update an existing key
	err = batch.Put(ctx, datastore.NewKey("EXISTING3"), []byte("updated:EXISTING3"))
	if err != nil {
		t.Fatal(err)
	}

	// Query the batch - should see batch changes
	batchReader, ok := batch.(flatfs.BatchReader)
	if !ok {
		t.Fatal("batch should implement BatchReader")
	}

	q := query.Query{}
	results, err := batchReader.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	entries := collectQueryResults(t, results)

	// Should have:
	// - /EXISTING1 (from main)
	// - /EXISTING3 (updated in batch)
	// - /BATCH1, /BATCH2 (new in batch)
	// Should NOT have:
	// - /EXISTING2 (deleted in batch)

	expectedKeys := map[string]string{
		"/EXISTING1": "main:EXISTING1",
		"/EXISTING3": "updated:EXISTING3",
		"/BATCH1":    "batch:BATCH1",
		"/BATCH2":    "batch:BATCH2",
	}

	if len(entries) != len(expectedKeys) {
		t.Fatalf("expected %d entries, got %d", len(expectedKeys), len(entries))
	}

	for _, entry := range entries {
		expected, ok := expectedKeys[entry.Key]
		if !ok {
			t.Errorf("unexpected key: %s", entry.Key)
			continue
		}
		if string(entry.Value) != expected {
			t.Errorf("value mismatch for key %s: expected %s, got %s", entry.Key, expected, string(entry.Value))
		}
		delete(expectedKeys, entry.Key)
	}

	if len(expectedKeys) > 0 {
		t.Errorf("missing keys in query results: %v", expectedKeys)
	}

	// Test KeysOnly query
	q = query.Query{KeysOnly: true}
	results, err = batchReader.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	entries = collectQueryResults(t, results)
	if len(entries) != 4 {
		t.Errorf("expected 4 keys, got %d", len(entries))
	}
	for _, entry := range entries {
		if entry.Value != nil {
			t.Error("KeysOnly query should not return values")
		}
	}

	// Test ReturnsSizes query
	q = query.Query{KeysOnly: true, ReturnsSizes: true}
	results, err = batchReader.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	entries = collectQueryResults(t, results)
	if len(entries) != 4 {
		t.Errorf("expected 4 keys, got %d", len(entries))
	}
	for _, entry := range entries {
		if entry.Size <= 0 {
			t.Error("ReturnsSizes query should return sizes")
		}
		if entry.Value != nil {
			t.Error("KeysOnly query should not return values")
		}
	}

	// Commit the batch
	err = batch.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Query main datastore - should see committed changes
	q = query.Query{}
	results, err = fs.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	entries = collectQueryResults(t, results)
	if len(entries) != 4 {
		t.Errorf("expected 4 entries after commit, got %d", len(entries))
	}

	// Verify committed data
	for _, entry := range entries {
		switch entry.Key {
		case "/EXISTING1":
			if string(entry.Value) != "main:EXISTING1" {
				t.Errorf("expected main:EXISTING1, got %s", string(entry.Value))
			}
		case "/EXISTING3":
			if string(entry.Value) != "updated:EXISTING3" {
				t.Errorf("expected updated:EXISTING3, got %s", string(entry.Value))
			}
		case "/BATCH1":
			if string(entry.Value) != "batch:BATCH1" {
				t.Errorf("expected batch:BATCH1, got %s", string(entry.Value))
			}
		case "/BATCH2":
			if string(entry.Value) != "batch:BATCH2" {
				t.Errorf("expected batch:BATCH2, got %s", string(entry.Value))
			}
		default:
			t.Errorf("unexpected key after commit: %s", entry.Key)
		}
	}

	// Verify /EXISTING2 was deleted
	has, err := fs.Has(ctx, datastore.NewKey("EXISTING2"))
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Error("/EXISTING2 should be deleted")
	}
}

func collectQueryResults(t *testing.T, results query.Results) []query.Entry {
	t.Helper()
	entries, err := results.Rest()
	if err != nil {
		t.Fatalf("query result error: %v", err)
	}
	return entries
}
