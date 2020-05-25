package kv

import (
	"github.com/jrife/ptarmigan/utils/numbers"
)

const (
	defaultKeysCap = 100
	maxKeysCap     = 1000
)

// SortOrder describes sort order for iteration
// Either SortOrderAsc or SortOrderDesc
type SortOrder int

// SortOrderAsc sorts in increasing order
const SortOrderAsc SortOrder = 0

// SortOrderDesc sorts in decreasing order
const SortOrderDesc SortOrder = 1

// PluginOptions is a generic structure to pass
// configuration to a storage plugin
type PluginOptions map[string]interface{}

// KV represents a key-value pair
// [0] is the key
// [1] is the value
type KV [2][]byte

// Key returns the key
func (kv KV) Key() []byte {
	return kv[0]
}

// Value returns the value
func (kv KV) Value() []byte {
	return kv[1]
}

// Keys reads up to limit keys from the iterator
// and returns them in a slice of KVs. If limit
// is negative or zero there is no limit, Keys
// will read all keys from the iterator.
func Keys(iter Iterator, limit int) ([]KV, error) {
	var kvs []KV

	if limit >= 0 {
		kvs = make([]KV, 0, numbers.Min(limit, maxKeysCap))
	} else {
		kvs = make([]KV, 0, defaultKeysCap)
	}

	for iter.Next() {
		kvs = append(kvs, KV{iter.Key(), iter.Value()})
	}

	return kvs, iter.Error()
}
