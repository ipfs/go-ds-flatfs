// Package flatfs is a Datastore implementation that stores all
// objects in a two-level directory structure in the local file
// system, regardless of the hierarchy of the keys.
package flatfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jbenet/go-os-rename"
)

func UpgradeV0toV1(path string, prefixLen int) error {
	fun := Prefix(prefixLen)
	err := WriteShardFunc(path, fun)
	if err != nil {
		return err
	}
	err = WriteReadme(path, fun)
	if err != nil {
		return err
	}
	return nil
}

func DowngradeV1toV0(path string) error {
	err := os.Remove(filepath.Join(path, SHARDING_FN))
	if err != nil {
		return err
	}
	err = os.Remove(filepath.Join(path, README_FN))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func Move(oldPath string, newPath string, out io.Writer) error {
	oldDS, err := Open(oldPath, false)
	if err != nil {
		return fmt.Errorf("%s: %v", oldPath, err)
	}
	newDS, err := Open(newPath, false)
	if err != nil {
		return fmt.Errorf("%s: %v", newPath, err)
	}

	if out != nil {
		fmt.Fprintf(out, "Getting Keys...\n")
	}
	res, err := oldDS.Query(query.Query{KeysOnly: true})
	if err != nil {
		return err
	}
	entries, err := res.Rest()
	if err != nil {
		return err
	}
	prog := newProgress(len(entries), out)

	if out != nil {
		fmt.Fprintf(out, "Moving Keys...\n")
	}

	// first move the keys
	for _, e := range entries {
		err := moveKey(oldDS, newDS, datastore.RawKey(e.Key))
		if err != nil {
			return err
		}
		prog.Next()
	}

	if out != nil {
		fmt.Fprintf(out, "\nCleaning Up...\n")
	}

	// now walk the old top-level directory
	dir, err := os.Open(oldDS.path)
	if err != nil {
		return err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, fn := range names {
		if fn == "." || fn == ".." {
			continue
		}
		oldPath := filepath.Join(oldDS.path, fn)
		inf, err := os.Stat(oldPath)
		if err != nil {
			return err
		}
		if inf.IsDir() || fn == "SHARDING" || fn == "_README" {
			// if we are a director or generated file just remove it
			err := os.Remove(oldPath)
			if err != nil {
				return err
			}
		} else {
			// else move it
			newPath := filepath.Join(newDS.path, fn)
			err := osrename.Rename(oldPath, newPath)
			if err != nil {
				return err
			}
		}
	}

	if out != nil {
		fmt.Fprintf(out, "All Done.\n")
	}

	return nil
}

func moveKey(oldDS *Datastore, newDS *Datastore, key datastore.Key) error {
	_, oldPath := oldDS.encode(key)
	dir, newPath := newDS.encode(key)
	err := newDS.makeDirNoSync(dir)
	if err != nil {
		return err
	}
	err = osrename.Rename(oldPath, newPath)
	if err != nil {
		return err
	}
	return nil
}

type progress struct {
	total   int
	current int

	out io.Writer

	start time.Time
}

func newProgress(total int, out io.Writer) *progress {
	return &progress{
		total: total,
		start: time.Now(),
		out:   out,
	}
}

func (p *progress) Next() {
	p.current++
	if p.out == nil {
		return
	}
	if p.current%10 == 0 || p.current == p.total {
		fmt.Fprintf(p.out, "\r[%d / %d]", p.current, p.total)
	}

	if p.current%100 == 0 || p.current == p.total {
		took := time.Now().Sub(p.start)
		av := took / time.Duration(p.current)
		estim := av * time.Duration(p.total-p.current)
		//est := strings.Split(estim.String(), ".")[0]

		fmt.Fprintf(p.out, "  Approx time remaining: %s  ", estim)
	}
}
