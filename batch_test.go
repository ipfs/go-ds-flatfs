package flatfs_test

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	flatfs "github.com/ipfs/go-ds-flatfs"
)

func TestBatchWritesToTempUntilCommit(t *testing.T) {
	tryAllShardFuncs(t, testBatchWritesToTempUntilCommit)
}

func testBatchWritesToTempUntilCommit(dirFunc mkShardFunc, t *testing.T) {
	temp := t.TempDir()
	defer checkTemp(t, temp)

	ffs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer ffs.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a batch
	batch, err := ffs.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Put some keys
	keys := []string{"QUUX", "QAAX", "QBBX"}
	for _, key := range keys {
		err = batch.Put(ctx, datastore.NewKey(key), []byte("testdata"))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Calling the Batch.Has method waits for async writes to finish.
	batchReader, ok := batch.(flatfs.BatchReader)
	if !ok {
		t.Fatal("batch does not implement BatchReader interface")
	}
	has, err := batchReader.Has(ctx, datastore.NewKey(keys[0]))
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("expected batch to have first key")
	}

	// Check that files don't exist in the main datastore yet
	for _, key := range keys {
		has, err := ffs.Has(ctx, datastore.NewKey(key))
		if err != nil {
			t.Fatal(err)
		}
		if has {
			t.Errorf("key %s should not exist in datastore before commit", key)
		}
	}

	// Check that no data files exist in shard directories
	err = filepath.WalkDir(temp, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".data" {
			relPath, _ := filepath.Rel(temp, path)
			// Ignore if it's in .temp directory
			if !isInTempDir(relPath) {
				return fmt.Errorf("found data file before commit: %s", relPath)
			}
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	// Now commit
	err = batch.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// After commit, all keys should exist
	for _, key := range keys {
		has, err := ffs.Has(ctx, datastore.NewKey(key))
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
	temp := t.TempDir()
	defer checkTemp(t, temp)

	ffs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer ffs.Close()

	// Put some initial data in the datastore
	initialKey := datastore.NewKey("INITIAL")
	initialData := []byte("initial data")
	err = ffs.Put(context.Background(), initialKey, initialData)
	if err != nil {
		t.Fatal(err)
	}

	// Create a batch
	batch, err := ffs.Batch(context.Background())
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
	err = ffs.Put(context.Background(), deleteKey, []byte("to be deleted"))
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
	data, err = ffs.Get(context.Background(), initialKey)
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
	data, err = ffs.Get(context.Background(), batchKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(batchData) {
		t.Errorf("expected %s, got %s", batchData, data)
	}

	// 2. Initial key should be overwritten
	data, err = ffs.Get(context.Background(), initialKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(overwriteData) {
		t.Errorf("expected %s, got %s", overwriteData, data)
	}

	// 3. Deleted key should not exist
	_, err = ffs.Get(context.Background(), deleteKey)
	if err != datastore.ErrNotFound {
		t.Errorf("expected ErrNotFound for deleted key after commit, got %v", err)
	}
}

func TestBatchDiscard(t *testing.T) {
	tryAllShardFuncs(t, testBatchDiscard)
}

func testBatchDiscard(dirFunc mkShardFunc, t *testing.T) {
	temp := t.TempDir()
	defer checkTemp(t, temp)

	ffs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer ffs.Close()

	// Create a batch
	batch, err := ffs.Batch(context.Background())
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
		has, err := ffs.Has(context.Background(), datastore.NewKey(key))
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
	temp := t.TempDir()
	defer checkTemp(t, temp)

	ffs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("CreateOrOpen fail: %v\n", err)
	}
	defer ffs.Close()

	ctx := context.Background()

	// Add some data to the main datastore
	mainKeys := []string{"EXISTING1", "EXISTING2", "EXISTING3"}
	for _, k := range mainKeys {
		err := ffs.Put(ctx, datastore.NewKey(k), []byte("main:"+k))
		if err != nil {
			t.Fatalf("Put fail: %v\n", err)
		}
	}

	// Create a batch
	batch, err := ffs.Batch(ctx)
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
	results, err = ffs.Query(ctx, q)
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
	has, err := ffs.Has(ctx, datastore.NewKey("EXISTING2"))
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

func TestConcurrentDuplicateBatchWrites(t *testing.T) {
	const (
		concurrency = 8
		numBatches  = 4
		numKeys     = 500
	)

	temp := t.TempDir()
	defer checkTemp(t, temp)

	ffs, err := flatfs.CreateOrOpen(temp, flatfs.Suffix(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer ffs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	batches := make([]datastore.Batch, 0, numBatches)
	for range cap(batches) {
		// Create a batch
		batch, err := ffs.Batch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, batch)
	}

	// Create keys
	keys := make([]string, 0, numKeys)
	for i := range cap(keys) {
		keys = append(keys, fmt.Sprintf("Q%03d", i))
	}
	// Create file content
	fileData := bytes.Repeat([]byte("testdata"), 256)

	start := make(chan struct{})
	var wgDone, wgReady sync.WaitGroup

	for _, batch := range batches {
		wgDone.Add(concurrency)
		wgReady.Add(concurrency)
		for range concurrency {
			go func() {
				defer wgDone.Done()
				wgReady.Done()
				<-start
				for _, key := range keys {
					if err := batch.Put(ctx, datastore.NewKey(key), fileData); err != nil {
						t.Fatal(err)
					}
				}
			}()
		}
	}

	wgReady.Wait() // wait for all goroutines to be ready
	close(start)   // run all goroutines
	wgDone.Wait()  // wait for all goroutines to finish

	if ctx.Err() != nil {
		t.Fatal("concurrent batch write did not complete")
	}

	for _, batch := range batches {
		// Calling the Batch.Has method waits for async writes to finish.
		batchReader, ok := batch.(flatfs.BatchReader)
		if !ok {
			t.Fatal("batch does not implement BatchReader interface")
		}
		has, err := batchReader.Has(ctx, datastore.NewKey(keys[0]))
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatal("expected batch to have first key")
		}
	}

	// Check that files don't exist in the main datastore yet
	for _, key := range keys {
		has, err := ffs.Has(ctx, datastore.NewKey(key))
		if err != nil {
			t.Fatal(err)
		}
		if has {
			t.Errorf("key %s should not exist in datastore before commit", key)
		}
	}

	var tempCount int
	err = filepath.WalkDir(temp, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".data" {
			relPath, _ := filepath.Rel(temp, path)
			if isInTempDir(relPath) {
				tempCount++
			}
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	// Check that there are the correct count for all batches.
	expectCount := len(batches) * len(keys)
	if tempCount != expectCount {
		t.Fatalf("wrong number of temp files, expected %d, got %d", expectCount, tempCount)
	}

	// Now commit concurrently.
	start = make(chan struct{})
	wgDone.Add(len(batches))
	wgReady.Add(len(batches))
	for _, batch := range batches {
		go func() {
			wgReady.Done()
			<-start
			defer wgDone.Done()
			err := batch.Commit(context.Background())
			if err != nil {
				t.Fatal(err)
			}
		}()
	}

	wgReady.Wait() // wait for all goroutines to be ready
	close(start)   // run all goroutines
	wgDone.Wait()  // wait for all goroutines to finish

	// After commit, all keys should exist and have correct data.
	for _, key := range keys {
		val, err := ffs.Get(ctx, datastore.NewKey(key))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(val, fileData) {
			t.Errorf("bad data for key %s", key)
		}
	}
}
