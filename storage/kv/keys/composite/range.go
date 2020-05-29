package composite

import (
	"github.com/jrife/ptarmigan/storage/kv/keys"
)

// All returns a new key range matching all keys
func All() Range {
	return Range{}
}

// Range represents all keys such that
//   k >= Min and k < Max
// If Min = nil that indicates the start of all keys
// If Max = nil that indicatese the end of all keys
// If multiple modifiers are called on a range the end
// result is effectively the same as ANDing all the
// restrictions.
type Range []keys.Range

// Get returns the range for the ith key level
// It returns a full range if level i doesn't exist
// in this range.
func (r Range) Get(i int) keys.Range {
	if i >= len(r) {
		return keys.All()
	}

	return r[i]
}

// Contains returns true if the range contains
// key
func (r Range) Contains(key Key) bool {
	for i := 0; i < len(key) && i < len(r); i++ {
		if !r[i].Contains(key[i]) {
			return false
		}
	}

	return true
}
