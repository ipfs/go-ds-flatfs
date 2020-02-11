package flatfs

import (
	"testing"

	"github.com/ipfs/go-datastore"
)

var (
	validKeys = []string{
		"/FOO",
		"/1BAR1",
		"/=EMACS-IS-KING=",
	}
	invalidKeys = []string{
		"/foo/bar",
		`/foo\bar`,
		"/foo\000bar",
		"/=Vim-IS-KING=",
	}
)

func TestKeyIsValid(t *testing.T) {
	for _, key := range validKeys {
		k := datastore.NewKey(key)
		if !keyIsValid(k) {
			t.Errorf("expected key %s to be valid", k)
		}
	}
	for _, key := range invalidKeys {
		k := datastore.NewKey(key)
		if keyIsValid(k) {
			t.Errorf("expected key %s to be invalid", k)
		}
	}
}
