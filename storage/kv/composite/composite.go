package composite

import (
	"fmt"

	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/keys"
	"github.com/jrife/ptarmigan/storage/kv/keys/composite"
)

// Node:
//   - key, value
//   - key, value
//   ...
// value can be literal or a node ref
// 0 is the root node id
// [8 byte node id].[key]
//

type MapUpdater struct {
	flatMap kv.Map
}

func (m *MapUpdater) Put(key composite.Key, value []byte) error {
	currentNodeId := keys.Int64ToKey(0)
	currentNode := kv.NamespaceMap(m.flatMap, currentNodeId[:])
	currentNodeMeta := kv.NamespaceMap(currentNode, []byte{0})
	currentNodeKeys := kv.NamespaceMap(currentNode, []byte{1})

	for _, k := range key {
		v, err := currentNode.Get(k)

		if err != nil {
			return fmt.Errorf("could not retrieve key %#v from node %#v", k, currentNode)
		}

		if v == nil {
			currentNode.Put()
		}
	}

	return nil
}

func (m *MapUpdater) Delete(key composite.Key, value []byte) error {
	return nil
}

type MapReader struct {
	flatMap kv.MapReader
}

func (m *MapReader) Get(key composite.Key) ([]byte, error) {
	return nil, nil
}

func (m *MapReader) Keys(keys keys.Range, order kv.SortOrder) (kv.CompositeKeyMapIterator, error) {
	return nil, nil
}

type Iterator struct {
	flatIter kv.Iterator
}

func (iter *Iterator) Next() bool {
	return false
}

func (iter *Iterator) Key() composite.Key {
	return composite.Key{}
}

func (iter *Iterator) Value() []byte {
	return nil
}

func (iter *Iterator) Error() error {
	return nil
}
