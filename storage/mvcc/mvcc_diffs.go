package mvcc

import (
	"github.com/jrife/ptarmigan/utils/numbers"
)

const (
	defaultDiffsCap = 100
	maxDiffsCap     = 1000
)

// Diff is a tuple of key-value pairs
// [0] is the key
// [1] is the current value
// [2] is the previous value
type Diff [3][]byte

// Key returns the key
func (diff Diff) Key() []byte {
	return diff[0]
}

// Value returns the value
func (diff Diff) Value() []byte {
	return diff[1]
}

// Prev returns the previous value
func (diff Diff) Prev() []byte {
	return diff[2]
}

// Diffs reads up to limit keys from the iterator
// and returns them in a slice of Diffs. If limit
// is negative or zero there is no limit, Diffs
// will read all keys from the iterator.
func Diffs(iter DiffIterator, limit int) ([]Diff, error) {
	var diffs []Diff

	if limit >= 0 {
		diffs = make([]Diff, 0, numbers.Min(limit, maxDiffsCap))
	} else {
		diffs = make([]Diff, 0, defaultDiffsCap)
	}

	for iter.Next() {
		diffs = append(diffs, Diff{iter.Key(), iter.Value(), iter.Prev()})
	}

	return diffs, iter.Error()
}
