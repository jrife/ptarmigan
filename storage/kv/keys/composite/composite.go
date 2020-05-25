package composite

import (
	"github.com/jrife/ptarmigan/storage/kv/keys"
	"github.com/jrife/ptarmigan/utils/sequence"
)

var _ sequence.Interface = (Key)(nil)

// Key is a sequence of keys
type Key [][]byte

// Len implements sequence.Interface.Len
func (key Key) Len() int {
	return len(key)
}

// Get implements sequence.Interface.Get
func (key Key) Get(i int) sequence.Element {
	return keys.Key(key[i])
}

// Set implements sequence.Interface.Set
func (key Key) Set(i int, e sequence.Element) {
	key[i] = e.(keys.Key)
}

// Inc implements sequence.Interface.Inc
func (key Key) Inc() sequence.Interface {
	return sequence.Inc(key)
}

// Copy implements sequence.Interface.Copy
func (key Key) Copy() sequence.Interface {
	cp := make(Key, len(key))

	copy(cp, key)

	return cp
}

// AppendMinElement implements sequence.Interface.AppendMinElement
func (key Key) AppendMinElement() sequence.Interface {
	return append(key, keys.Key{})
}

// Compare compares two keys
// -1 means a < b
// 1 means a > b
// 0 means a = b
func Compare(a, b Key) int {
	return sequence.Compare(a, b)
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
