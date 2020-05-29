package kv

import (
	"bytes"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/jrife/flock/storage/kv/keys"
)

var _ Map = (*FakeMap)(nil)

// FakeMap is an in-memory implementation of the
// Map interface
type FakeMap struct {
	m *treemap.Map
}

// NewFakeMap creates a new FakeMap
func NewFakeMap() Map {
	return &FakeMap{m: treemap.NewWith(func(a, b interface{}) int {
		return bytes.Compare(a.([]byte), b.([]byte))
	})}
}

// Put implements Map.Put
func (m *FakeMap) Put(key, value []byte) error {
	m.m.Put(key, value)

	return nil
}

// Delete implements Map.Delete
func (m *FakeMap) Delete(key []byte) error {
	m.m.Remove(key)

	return nil
}

// Get implements Map.Get
func (m *FakeMap) Get(key []byte) ([]byte, error) {
	v, ok := m.m.Get(key)

	if !ok {
		return nil, nil
	}

	return v.([]byte), nil
}

// Keys implements Map.Keys
func (m *FakeMap) Keys(keys keys.Range, order SortOrder) (Iterator, error) {
	iter := m.m.Iterator()

	if order == SortOrderDesc {
		iter.End()
	} else {
		iter.Begin()
	}

	return &FakeIterator{iter: iter, keys: keys, order: order}, nil
}

var _ Iterator = (*FakeIterator)(nil)

// FakeIterator is the iterator implementation for FakeMap
type FakeIterator struct {
	iter  treemap.Iterator
	keys  keys.Range
	order SortOrder
}

// Next implements Iterator.Next
func (iter *FakeIterator) Next() bool {
	hasMore := false

	if iter.order == SortOrderDesc {
		for hasMore = iter.iter.Prev(); hasMore && (iter.keys.Max == nil || keys.Compare(iter.iter.Key().([]byte), iter.keys.Max) >= 0); hasMore = iter.iter.Prev() {
		}

		if !hasMore || iter.keys.Min != nil && keys.Compare(iter.iter.Key().([]byte), iter.keys.Min) < 0 {
			return false
		}
	} else {
		for hasMore = iter.iter.Next(); hasMore && (iter.keys.Min == nil || keys.Compare(iter.iter.Key().([]byte), iter.keys.Min) < 0); hasMore = iter.iter.Next() {
		}

		if !hasMore || iter.keys.Max != nil && keys.Compare(iter.iter.Key().([]byte), iter.keys.Max) >= 0 {
			return false
		}
	}

	return true
}

// Key implements Iterator.Key
func (iter *FakeIterator) Key() []byte {
	return iter.iter.Key().([]byte)
}

// Value implements Iterator.Value
func (iter *FakeIterator) Value() []byte {
	return iter.iter.Value().([]byte)
}

// Error implements Iterator.Error
func (iter *FakeIterator) Error() error {
	return nil
}
