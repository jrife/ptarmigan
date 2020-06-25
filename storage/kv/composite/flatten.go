package composite

import (
	"fmt"

	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/storage/kv/keys/composite"
)

// FlattenMap converts a composite map
// to a flat map
func FlattenMap(m Map) kv.Map {
	return &flattenedMap{m: m}
}

type flattenedMap struct {
	m Map
}

func (m *flattenedMap) Put(key, value []byte) error {
	return m.m.Put(composite.Key{key}, value)
}

func (m *flattenedMap) Delete(key []byte) error {
	return m.m.Delete(composite.Key{key})
}

func (m *flattenedMap) Get(key []byte) ([]byte, error) {
	return m.m.Get(composite.Key{key})
}

func (m *flattenedMap) Keys(keys keys.Range, order kv.SortOrder) (kv.Iterator, error) {
	iter, err := m.m.Keys(composite.Range{keys}, order)

	if err != nil {
		return nil, err
	}

	return &flattenedIter{iter: iter}, nil
}

type flattenedIter struct {
	iter Iterator
	err  error
}

func (iter *flattenedIter) Next() bool {
	if !iter.iter.Next() {
		iter.err = iter.iter.Error()
		return false
	}

	if len(iter.iter.Key()) != 1 {
		iter.err = fmt.Errorf("iteration is not over a flattenable map: composite key detected: %#v", iter.iter.Key())
		return false
	}

	return true
}

func (iter *flattenedIter) Key() []byte {
	return iter.iter.Key()[0]
}

func (iter *flattenedIter) Value() []byte {
	return iter.iter.Value()
}

func (iter *flattenedIter) Error() error {
	return iter.err
}
