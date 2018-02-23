// Package flatfs is a Datastore implementation that stores all
// objects in a two-level directory structure in the local file
// system, regardless of the hierarchy of the keys.
package flatfs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jbenet/go-os-rename"

	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("flatfs")

const (
	extension     = ".data"
	diskUsageFile = "diskUsage.cache"
)

const (
	putOp = iota
	deleteOp
	renameOp
)

type opT int

// op wraps useful arguments of write operations
type op struct {
	typ  opT           // operation type
	key  datastore.Key // datastore key. Mandatory.
	tmp  string        // temp file path
	path string        // file path
	v    []byte        // value
}

// Datastore implements the go-datastore Interface.
// Note this datastore cannot guarantee order of concurrent
// write operations to the same key. See the explanation in
// Put().
type Datastore struct {
	path string

	shardStr string
	getDir   ShardFunc

	// sychronize all writes and directory changes for added safety
	sync bool

	diskUsage int64

	// opMap handles concurrent write operations (put/delete)
	// to the same key
	opMap     map[string]*sync.Mutex
	opMapLock sync.RWMutex
}

type ShardFunc func(string) string

var _ datastore.Datastore = (*Datastore)(nil)

var (
	ErrDatastoreExists       = errors.New("datastore already exist")
	ErrDatastoreDoesNotExist = errors.New("datastore directory does not exist")
	ErrShardingFileMissing   = fmt.Errorf("%s file not found in datastore", SHARDING_FN)
)

func Create(path string, fun *ShardIdV1) error {

	err := os.Mkdir(path, 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	dsFun, err := ReadShardFunc(path)
	switch err {
	case ErrShardingFileMissing:
		isEmpty, err := DirIsEmpty(path)
		if err != nil {
			return err
		}
		if !isEmpty {
			return fmt.Errorf("directory missing %s file: %s", SHARDING_FN, path)
		}

		err = WriteShardFunc(path, fun)
		if err != nil {
			return err
		}
		err = WriteReadme(path, fun)
		return err
	case nil:
		if fun.String() != dsFun.String() {
			return fmt.Errorf("specified shard func '%s' does not match repo shard func '%s'",
				fun.String(), dsFun.String())
		}
		return ErrDatastoreExists
	default:
		return err
	}
}

func Open(path string, syncFiles bool) (*Datastore, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, ErrDatastoreDoesNotExist
	} else if err != nil {
		return nil, err
	}

	shardId, err := ReadShardFunc(path)
	if err != nil {
		return nil, err
	}

	fs := &Datastore{
		path:      path,
		shardStr:  shardId.String(),
		getDir:    shardId.Func(),
		sync:      syncFiles,
		diskUsage: 0,
		opMap:     make(map[string]*sync.Mutex),
	}

	// This sets diskUsage to the correct value
	// It might be slow, but allowing it to happen
	// while the datastore is usable might
	// cause diskUsage to not be accurate.
	err = fs.calculateDiskUsage()
	if err != nil {
		// Cannot stat() all
		// elements in the datastore.
		return nil, err
	}
	return fs, nil
}

// convenience method
func CreateOrOpen(path string, fun *ShardIdV1, sync bool) (*Datastore, error) {
	err := Create(path, fun)
	if err != nil && err != ErrDatastoreExists {
		return nil, err
	}
	return Open(path, sync)
}

func (fs *Datastore) ShardStr() string {
	return fs.shardStr
}

func (fs *Datastore) encode(key datastore.Key) (dir, file string) {
	noslash := key.String()[1:]
	dir = filepath.Join(fs.path, fs.getDir(noslash))
	file = filepath.Join(dir, noslash+extension)
	return dir, file
}

func (fs *Datastore) decode(file string) (key datastore.Key, ok bool) {
	if filepath.Ext(file) != extension {
		return datastore.Key{}, false
	}
	name := file[:len(file)-len(extension)]
	return datastore.NewKey(name), true
}

func (fs *Datastore) makeDir(dir string) error {
	if err := fs.makeDirNoSync(dir); err != nil {
		return err
	}

	// In theory, if we create a new prefix dir and add a file to
	// it, the creation of the prefix dir itself might not be
	// durable yet. Sync the root dir after a successful mkdir of
	// a prefix dir, just to be paranoid.
	if fs.sync {
		if err := syncDir(fs.path); err != nil {
			return err
		}
	}
	return nil
}

func (fs *Datastore) makeDirNoSync(dir string) error {
	if err := os.Mkdir(dir, 0755); err != nil {
		// EEXIST is safe to ignore here, that just means the prefix
		// directory already existed.
		if !os.IsExist(err) {
			return err
		}
		return nil
	}

	// Track DiskUsage of this NEW folder
	fs.updateDiskUsage(dir, true)
	return nil
}

// This function always runs under an opLock. Therefore, only one thread is
// touching the affected files.
func (fs *Datastore) renameAndUpdateDiskUsage(tmpPath, path string) error {
	fi, err := os.Stat(path)

	// Destination exists, we need to discount it from diskUsage
	if fs != nil && err == nil {
		atomic.AddInt64(&fs.diskUsage, -fi.Size())
	} else if !os.IsNotExist(err) {
		return err
	}

	// Rename and add new file's diskUsage
	if err := osrename.Rename(tmpPath, path); err != nil {
		return err
	}
	fs.updateDiskUsage(path, true)
	return nil
}

var putMaxRetries = 6

// Put stores a key/value in the datastore.
//
// Note, that we do not guarantee order of write operations (Put or Delete)
// to the same key in this datastore.
//
// For example. i.e. in the case of two concurrent Put, we only guarantee
// that one of them will come through, but cannot assure which one even if
// one arrived slightly later than the other. In the case of a
// concurrent Put and a Delete operation, we cannot guarantee which one
// will win.
func (fs *Datastore) Put(key datastore.Key, value interface{}) error {
	val, ok := value.([]byte)
	if !ok {
		return datastore.ErrInvalidType
	}

	var err error
	for i := 1; i <= putMaxRetries; i++ {
		err = fs.doWriteOp(&op{
			typ: putOp,
			key: key,
			v:   val,
		})
		if err == nil {
			break
		}

		if !strings.Contains(err.Error(), "too many open files") {
			break
		}

		log.Errorf("too many open files, retrying in %dms", 100*i)
		time.Sleep(time.Millisecond * 100 * time.Duration(i))
	}
	return err
}

func (fs *Datastore) doOp(oper *op) error {
	switch oper.typ {
	case putOp:
		return fs.doPut(oper.key, oper.v)
	case deleteOp:
		return fs.doDelete(oper.key)
	case renameOp:
		return fs.renameAndUpdateDiskUsage(oper.tmp, oper.path)
	default:
		panic("bad operation, this is a bug")
		return errors.New("bad operation, this is a bug")
	}
}

// doWrite optmizes out write operations (put/delete) to the same
// key by queueing them and suceeding all queued
// operations if one of them does. In such case,
// we assume that the first suceeding operation
// on that key was the last one to happen after
// two successful others.
func (fs *Datastore) doWriteOp(oper *op) error {
	keyStr := oper.key.String()

	// Log that we are performing an operation
	// on key
	fs.opMapLock.Lock()
	opLock, ok := fs.opMap[keyStr]
	if !ok {
		opLock = &sync.Mutex{}
		fs.opMap[keyStr] = opLock
	}
	fs.opMapLock.Unlock()

	// Attempt to run the operation.
	// Only one operation can run at a time
	// on the same key. We use opLock
	// for that.
	opLock.Lock()
	defer func() {
		opLock.Unlock()
	}()

	// If we have the opLock but the
	// opMapLock does not contain our key, or it does
	// but it points to a different lock (!),
	// it means some other concurrent operation from our batch
	// cleared it upon success. We succeed in this case.
	fs.opMapLock.RLock()
	newLock, ok := fs.opMap[keyStr]
	fs.opMapLock.RUnlock()
	if !ok || newLock != opLock {
		return nil
	}

	// Otherwise, either there are no other concurrent
	// operations, or they failed. Which means it's our
	// turn to run.
	err := fs.doOp(oper)
	if err != nil {
		// We failed. Tell the user. Leave the operation
		// in opMap so that other potentially concurrent operations
		// can try.
		// Note: opMap will contain keys for operations which just
		// failed, even if they did not compete with another
		// one. If datastore operations fail all the time
		// opMap will grow as it retains all keys of failed operations
		// and only deletes them on success.
		return err
	}

	//time.Sleep(2 * time.Second)

	// In case of succees, we signal all other
	// concurrent operations that they don't need to run
	// by cleaning the key from the opMap
	fs.opMapLock.Lock()
	delete(fs.opMap, keyStr)
	fs.opMapLock.Unlock()
	return nil
}

func (fs *Datastore) doPut(key datastore.Key, val []byte) error {

	dir, path := fs.encode(key)
	if err := fs.makeDir(dir); err != nil {
		return err
	}

	tmp, err := ioutil.TempFile(dir, "put-")
	if err != nil {
		return err
	}
	closed := false
	removed := false
	defer func() {
		if !closed {
			// silence errcheck
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}
	}()

	if _, err := tmp.Write(val); err != nil {
		return err
	}
	if fs.sync {
		if err := syncFile(tmp); err != nil {
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true

	err = fs.renameAndUpdateDiskUsage(tmp.Name(), path)
	if err != nil {
		return err
	}
	removed = true

	if fs.sync {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func (fs *Datastore) putMany(data map[datastore.Key]interface{}) error {
	var dirsToSync []string
	files := make(map[*os.File]*op)

	for key, value := range data {
		val, ok := value.([]byte)
		if !ok {
			return datastore.ErrInvalidType
		}
		dir, path := fs.encode(key)
		if err := fs.makeDirNoSync(dir); err != nil {
			return err
		}
		dirsToSync = append(dirsToSync, dir)

		tmp, err := ioutil.TempFile(dir, "put-")
		if err != nil {
			return err
		}

		if _, err := tmp.Write(val); err != nil {
			return err
		}

		files[tmp] = &op{
			typ:  renameOp,
			path: path,
			tmp:  tmp.Name(),
			key:  key,
		}
	}

	ops := make(map[*os.File]int)

	defer func() {
		for fi, _ := range files {
			val, _ := ops[fi]
			switch val {
			case 0:
				_ = fi.Close()
				fallthrough
			case 1:
				_ = os.Remove(fi.Name())
			}
		}
	}()

	// Now we sync everything
	// sync and close files
	for fi, _ := range files {
		if fs.sync {
			if err := syncFile(fi); err != nil {
				return err
			}
		}

		if err := fi.Close(); err != nil {
			return err
		}

		// signify closed
		ops[fi] = 1
	}

	// move files to their proper places
	for fi, op := range files {
		fs.doWriteOp(op)
		// signify removed
		ops[fi] = 2
	}

	// now sync the dirs for those files
	if fs.sync {
		for _, dir := range dirsToSync {
			if err := syncDir(dir); err != nil {
				return err
			}
		}

		// sync top flatfs dir
		if err := syncDir(fs.path); err != nil {
			return err
		}
	}

	return nil
}

func (fs *Datastore) Get(key datastore.Key) (value interface{}, err error) {
	_, path := fs.encode(key)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, datastore.ErrNotFound
		}
		// no specific error to return, so just pass it through
		return nil, err
	}
	return data, nil
}

func (fs *Datastore) Has(key datastore.Key) (exists bool, err error) {
	_, path := fs.encode(key)
	switch _, err := os.Stat(path); {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

// Delete removes a key/value from the Datastore. Please read
// the Put() explanation about the handling of concurrent write
// operations to the same key.
func (fs *Datastore) Delete(key datastore.Key) error {
	return fs.doWriteOp(&op{
		typ: deleteOp,
		key: key,
		v:   nil,
	})
}

// This function always runs within an opLock for the given
// key, and not concurrently.
func (fs *Datastore) doDelete(key datastore.Key) error {
	_, path := fs.encode(key)

	fSize := fileSize(path)

	switch err := os.Remove(path); {
	case err == nil:
		atomic.AddInt64(&fs.diskUsage, -fSize)
		return nil
	case os.IsNotExist(err):
		return datastore.ErrNotFound
	default:
		return err
	}
}

func (fs *Datastore) Query(q query.Query) (query.Results, error) {
	if (q.Prefix != "" && q.Prefix != "/") ||
		len(q.Filters) > 0 ||
		len(q.Orders) > 0 ||
		q.Limit > 0 ||
		q.Offset > 0 ||
		!q.KeysOnly {
		// TODO this is overly simplistic, but the only caller is
		// `ipfs refs local` for now, and this gets us moving.
		return nil, errors.New("flatfs only supports listing all keys in random order")
	}

	reschan := make(chan query.Result, query.KeysOnlyBufSize)
	go func() {
		defer close(reschan)
		err := fs.walkTopLevel(fs.path, reschan)
		if err != nil {
			reschan <- query.Result{Error: errors.New("walk failed: " + err.Error())}
		}
	}()
	return query.ResultsWithChan(q, reschan), nil
}

func (fs *Datastore) walkTopLevel(path string, reschan chan query.Result) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, dir := range names {

		if len(dir) == 0 || dir[0] == '.' {
			continue
		}

		err = fs.walk(filepath.Join(path, dir), reschan)
		if err != nil {
			return err
		}

	}
	return nil
}

// calculateDiskUsage tries to read the diskUsageFile for a cached
// diskUsage value, otherwise walks the datastore files.
func (fs *Datastore) calculateDiskUsage() error {
	// Try to obtain a previously stored value from disk
	if persDu := fs.readDiskUsageFile(); persDu > 0 {
		atomic.StoreInt64(&fs.diskUsage, persDu)
		return nil
	}

	// Otherwise walk the datastore files
	var du int64
	err := filepath.Walk(fs.path,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			du += info.Size()
			return nil
		})

	if err != nil {
		log.Errorf("cannot calculate disk usage: %s", err)
		return err
	}

	atomic.StoreInt64(&fs.diskUsage, du)
	return nil
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// updateDiskUsage reads the size of path and atomically
// increases or decreases the diskUsage variable.
// setting add to false will subtract from disk usage.
func (fs *Datastore) updateDiskUsage(path string, add bool) {
	fsize := fileSize(path)
	if !add {
		fsize = -fsize
	}

	if fsize != 0 {
		atomic.AddInt64(&fs.diskUsage, fsize)
	}
}

func (fs *Datastore) persistDiskUsageFile() {
	// Store DiskUsage on clean shutdowns.
	du, err := fs.DiskUsage()
	if err != nil {
		// do not store on errors. just ignore them
		return
	}

	duB := []byte(fmt.Sprintf("%d", du))
	tmp, err := ioutil.TempFile(fs.path, "du-")
	if err != nil {
		return
	}
	if _, err := tmp.Write(duB); err != nil {
		return
	}
	if err := tmp.Close(); err != nil {
		return
	}

	osrename.Rename(tmp.Name(), filepath.Join(fs.path, diskUsageFile))
}

// deletes the diskUsageFile after reading
func (fs *Datastore) readDiskUsageFile() int64 {
	fpath := filepath.Join(fs.path, diskUsageFile)
	defer os.Remove(fpath)
	duB, err := ioutil.ReadFile(fpath)
	if err != nil {
		return 0
	}
	i, err := strconv.ParseInt(string(duB), 10, 64)
	if err != nil {
		return 0
	}
	return i
}

// DiskUsage implements the PersistentDatastore interface
// and returns the current disk usage in bytes used by
// this datastore.
//
// The size is approximative and may slightly differ from
// the real disk values.
func (fs *Datastore) DiskUsage() (uint64, error) {
	// it may differ from real disk values if
	// the filesystem has allocated for blocks
	// for a directory because it has many files in it
	// we don't account for "resized" directories.
	// In a large datastore, the differences should be
	// are negligible though.

	du := atomic.LoadInt64(&fs.diskUsage)
	return uint64(du), nil
}

func (fs *Datastore) walk(path string, reschan chan query.Result) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()

	// ignore non-directories
	fileInfo, err := dir.Stat()
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return nil
	}

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, fn := range names {

		if len(fn) == 0 || fn[0] == '.' {
			continue
		}

		key, ok := fs.decode(fn)
		if !ok {
			log.Warningf("failed to decode flatfs entry: %s", fn)
			continue
		}

		reschan <- query.Result{
			Entry: query.Entry{
				Key: key.String(),
			},
		}
	}
	return nil
}

func (fs *Datastore) Close() error {
	fs.persistDiskUsageFile()
	return nil
}

type flatfsBatch struct {
	puts    map[datastore.Key]interface{}
	deletes map[datastore.Key]struct{}

	ds *Datastore
}

func (fs *Datastore) Batch() (datastore.Batch, error) {
	return &flatfsBatch{
		puts:    make(map[datastore.Key]interface{}),
		deletes: make(map[datastore.Key]struct{}),
		ds:      fs,
	}, nil
}

func (bt *flatfsBatch) Put(key datastore.Key, val interface{}) error {
	bt.puts[key] = val
	return nil
}

func (bt *flatfsBatch) Delete(key datastore.Key) error {
	bt.deletes[key] = struct{}{}
	return nil
}

func (bt *flatfsBatch) Commit() error {
	if err := bt.ds.putMany(bt.puts); err != nil {
		return err
	}

	for k, _ := range bt.deletes {
		if err := bt.ds.Delete(k); err != nil {
			return err
		}
	}

	return nil
}

var _ datastore.ThreadSafeDatastore = (*Datastore)(nil)

func (*Datastore) IsThreadSafe() {}
