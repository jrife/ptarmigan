package composite

import (
	"github.com/jrife/ptarmigan/storage/kv/keys"
)

// Key is a sequence of keys
type Key [][]byte

// Compare compares two keys
// -1 means a < b
// 1 means a > b
// 0 means a = b
func Compare(a, b Key) int {
	if a == nil && b != nil {
		return -1
	} else if a != nil && b == nil {
		return 1
	}

	var i int
	var cmp int

	for ; i < len(a) && i < len(b) && cmp == 0; cmp, i = keys.Compare(a[i], b[i]), i+1 {
	}

	if cmp != 0 {
		return cmp
	}

	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return 1
	}

	return 0
}

// HasPrefix returns true if
// b is a prefix of a
func HasPrefix(a, b Key) bool {
	if len(b) > len(a) {
		return false
	}

	for i, e := range b {
		if keys.Compare(a[i], e) != 0 {
			return false
		}
	}

	return true
}
