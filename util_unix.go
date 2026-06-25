//go:build !windows

package flatfs

import (
	"os"
)

func tempFileOnce(dir, pattern string) (*os.File, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	// os.CreateTemp hardcodes 0600; relax to 0666 so umask controls final permissions
	if err := f.Chmod(0666); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	return f, nil
}

func readFileOnce(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func createFile(name string) (*os.File, error) {
	return os.Create(name)
}
