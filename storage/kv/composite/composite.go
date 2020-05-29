package composite

import (
	"fmt"

	"github.com/jrife/flock/storage/kv"
	compositepb "github.com/jrife/flock/storage/kv/composite/pb"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/storage/kv/keys/composite"
)

const (
	rootNodeID int64 = 0
)

var (
	metaNs    = []byte{0}
	nodeIDKey = []byte{0}
	kvNs      = []byte{1}
)

// NewPartition returns an instance of composite.Partition
// built on top of a kv.Partition
func NewPartition(p kv.Partition) Partition {
	return &partition{Partition: p}
}

// NewMap returns an instance of composite.Map built on top
// of a kv.Map
func NewMap(m kv.Map, nextNodeID func() int64) Map {
	return &compositeMap{flatMap: m, nextNodeID: nextNodeID}
}

type node struct {
	compositepb.Node
	ns []byte
}

type partition struct {
	kv.Partition
}

func (p *partition) Begin(writable bool) (Transaction, error) {
	flatTxn, err := p.Partition.Begin(writable)

	if err != nil {
		return nil, err
	}

	var nodeID int64
	var nodeIDArr [8]byte
	metadata := kv.NamespaceMap(flatTxn, metaNs)
	nodeIDValue, err := metadata.Get(nodeIDKey)

	if err != nil {
		return nil, err
	}

	if nodeIDValue != nil {
		if len(nodeIDValue) != 8 {
			return nil, fmt.Errorf("node ID key unexpected length: %#v", nodeIDValue)
		}

		copy(nodeIDArr[:], nodeIDValue)
		nodeID = keys.KeyToInt64(nodeIDArr)
	}

	return &transaction{
		flatTxn: flatTxn,
		Map: NewMap(flatTxn, func() int64 {
			nodeID = nodeID + 1

			return nodeID
		}),
		onCommit: func() error {
			nodeIDValue := keys.Int64ToKey(nodeID)
			if err := metadata.Put(nodeIDKey, nodeIDValue[:]); err != nil {
				return fmt.Errorf("could not update nodeID key: %s", err)
			}

			return nil
		},
	}, nil
}

type transaction struct {
	flatTxn kv.Transaction
	Map
	onCommit func() error
}

func (txn *transaction) Metadata() ([]byte, error) {
	return txn.flatTxn.Metadata()
}

func (txn *transaction) SetMetadata(metadata []byte) error {
	return txn.flatTxn.SetMetadata(metadata)
}

func (txn *transaction) Commit() error {
	if txn.onCommit != nil {
		if err := txn.onCommit(); err != nil {
			txn.Rollback()

			return err
		}
	}

	return txn.flatTxn.Commit()
}

func (txn *transaction) Rollback() error {
	return txn.flatTxn.Rollback()
}

var _ Map = (*compositeMap)(nil)

type compositeMap struct {
	flatMap    kv.Map
	nextNodeID func() int64
}

// nodes returns a list of nodes corresponding to each element in the key
// for each i in key, nodes[i].ChildRef as well as ns is a reference to the node id
// where key[i] exists. If includeKeysNode is true, the resulting list's length
// is one more than  the length of key and the last element contains a reference to the
// node where children of the last key element would reside. If create is true then
// nodes ensures that a node exists for each key element. If it's false the nodes list
// may be shorter than key if the key does not exist.
func (m *compositeMap) nodes(key composite.Key, create bool, includeKeysNode bool) ([]node, error) {
	var nodes []node

	if includeKeysNode {
		nodes = make([]node, 0, len(key)+1)
	} else {
		nodes = make([]node, 0, len(key))
	}

	nodes = append(nodes, node{Node: compositepb.Node{ChildRef: 0}})

	for i, k := range key {
		if len(k) == 0 {
			return nil, fmt.Errorf("key component cannot be empty or nil")
		}

		currentNodeID := nodes[len(nodes)-1].ChildRef
		currentNodeNs := keys.Int64ToKey(currentNodeID)
		nodes[len(nodes)-1].ns = currentNodeNs[:]

		if i == len(key)-1 && !includeKeysNode {
			break
		}

		currentNode := kv.NamespaceMap(m.flatMap, nodes[len(nodes)-1].ns)

		v, err := currentNode.Get(k)

		if err != nil {
			return nil, fmt.Errorf("could not retrieve key %#v from node %#v: %s", k, currentNodeID, err)
		}

		var n compositepb.Node

		if v == nil {
			if create {
				n.ChildRef = m.nextNodeID()
				marshaled, err := n.Marshal()

				if err != nil {
					return nil, fmt.Errorf("could not marshal node %#v: %s", n, err)
				}

				if err := currentNode.Put(k, marshaled); err != nil {
					return nil, err
				}
			} else {
				break
			}
		} else {
			if err := n.Unmarshal(v); err != nil {
				return nil, err
			}

			if create && n.ChildRef == 0 {
				n.ChildRef = m.nextNodeID()
				marshaled, err := n.Marshal()

				if err != nil {
					return nil, fmt.Errorf("could not marshal node %#v: %s", n, err)
				}

				if err := currentNode.Put(k, marshaled); err != nil {
					return nil, err
				}
			}
		}

		nodes = append(nodes, node{Node: n})
	}

	return nodes, nil
}

// Put implements kv.CompositeMapUpdater.Put
func (m *compositeMap) Put(key composite.Key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty or nil")
	}

	if value == nil {
		return fmt.Errorf("value cannot be nil")
	}

	nodes, err := m.nodes(key, true, false)

	if err != nil {
		return err
	}

	v, err := kv.NamespaceMap(m.flatMap, nodes[len(key)-1].ns).Get(key[len(key)-1])

	if err != nil {
		return fmt.Errorf("could not retrieve key %#v from node %#v: %s", key[len(key)-1], nodes[len(key)-1].ChildRef, err)
	}

	var n compositepb.Node

	if v != nil {
		if err := n.Unmarshal(v); err != nil {
			return fmt.Errorf("could not unmarshal key %#v from node %#v: %s", key[len(key)-1], nodes[len(key)-1].ChildRef, err)
		}
	}

	n.Value = value
	marshaledN, err := n.Marshal()

	if err != nil {
		return fmt.Errorf("could not marshal node %#v: %s", n, err)
	}

	return kv.NamespaceMap(m.flatMap, nodes[len(key)-1].ns).Put(key[len(key)-1], marshaledN)
}

// Delete implements kv.CompositeMapUpdater.Delete
func (m *compositeMap) Delete(key composite.Key) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty or nil")
	}

	nodes, err := m.nodes(key, false, true)

	if err != nil {
		return err
	}

	// This key doesn't exist
	if len(nodes) != len(key)+1 || nodes[len(nodes)-1].Value == nil {
		return nil
	}

	if nodes[len(nodes)-1].ChildRef != 0 {
		nodes[len(nodes)-1].Value = nil
		marshaledNode, err := nodes[len(nodes)-1].Marshal()

		if err != nil {
			return fmt.Errorf("could not marshal node %#v: %s", nodes[len(nodes)-2].ChildRef, err)
		}

		if err := kv.NamespaceMap(m.flatMap, nodes[len(nodes)-2].ns).Put(key[len(key)-1], marshaledNode); err != nil {
			return fmt.Errorf("could not update key %#v in node %d: %s", key[len(key)-1], nodes[len(nodes)-2].ChildRef, err)
		}

		return nil
	}

	for len(nodes) > 1 {
		// loop preconditions:
		// 1) there are no keys in node nodes[len(nodes)-1].ChildRef
		// 2) the key keys[len(key)-1] exists in node nodes[len(nodes)-2].ChildRef and its value is nil
		if err := kv.NamespaceMap(m.flatMap, nodes[len(nodes)-2].ns).Delete(key[len(key)-1]); err != nil {
			return fmt.Errorf("could not delete key %#v from node %d: %s", key[len(key)-1], nodes[len(nodes)-2].ChildRef, err)
		}

		key = key[:len(key)-1]
		nodes = nodes[:len(nodes)-1]

		if nodes[len(nodes)-1].Value != nil {
			break
		}

		empty, err := m.isNodeEmpty(kv.NamespaceMap(m.flatMap, nodes[len(nodes)-1].ns))

		if err != nil {
			return fmt.Errorf("could not check is node %d is empty: %s", nodes[len(nodes)-1].ChildRef, err)
		}

		if !empty {
			break
		}
	}

	return nil
}

func (m *compositeMap) isNodeEmpty(ns kv.Map) (bool, error) {
	iter, err := ns.Keys(keys.All(), kv.SortOrderAsc)

	if err != nil {
		return false, fmt.Errorf("could not create iterator: %s", err)
	}

	kvs, err := kv.Keys(iter, 1)

	if err != nil {
		return false, fmt.Errorf("could not read keys: %s", err)
	}

	return len(kvs) == 0, nil
}

// Get implements kv.CompositeMapReader.Get
func (m *compositeMap) Get(key composite.Key) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty or nil")
	}

	nodes, err := m.nodes(key, false, true)

	if err != nil {
		return nil, err
	}

	// This key doesn't exist
	if len(nodes) != len(key)+1 || nodes[len(nodes)-1].Value == nil {
		return nil, nil
	}

	return nodes[len(nodes)-1].Value, nil
}

// Keys implements kv.CompositeMapReader.Keys
func (m *compositeMap) Keys(keyRange composite.Range, order kv.SortOrder) (Iterator, error) {
	rootNs := keys.Int64ToKey(rootNodeID)
	rootIter, err := kv.NamespaceMap(m.flatMap, rootNs[:]).Keys(keyRange.Get(0), order)

	if err != nil {
		return nil, fmt.Errorf("could not create root iterator: %s", err)
	}

	iter := &iterator{
		flatMap:   m.flatMap,
		iterStack: []kv.Iterator{rootIter},
		keyStack:  composite.Key{nil},
		nodeStack: []compositepb.Node{{Value: nil, ChildRef: 0}},
		keyRange:  keyRange,
		order:     order,
	}

	return iter, nil
}

var _ Iterator = (*iterator)(nil)

type iterator struct {
	flatMap   kv.Map
	iterStack []kv.Iterator
	keyStack  composite.Key
	nodeStack []compositepb.Node
	keyRange  composite.Range
	key       composite.Key
	value     []byte
	err       error
	order     kv.SortOrder
}

func (iter *iterator) peek() (kv.Iterator, compositepb.Node) {
	if len(iter.iterStack) == 0 {
		return nil, compositepb.Node{}
	}

	return iter.iterStack[len(iter.iterStack)-1], iter.nodeStack[len(iter.nodeStack)-1]
}

func (iter *iterator) pop() (kv.Iterator, compositepb.Node) {
	if len(iter.iterStack) == 0 {
		return nil, compositepb.Node{}
	}

	i := iter.iterStack[len(iter.iterStack)-1]
	n := iter.nodeStack[len(iter.nodeStack)-1]
	iter.iterStack = iter.iterStack[:len(iter.iterStack)-1]
	iter.keyStack = iter.keyStack[:len(iter.keyStack)-1]
	iter.nodeStack = iter.nodeStack[:len(iter.nodeStack)-1]

	return i, n
}

func (iter *iterator) push(i kv.Iterator, k keys.Key, n compositepb.Node) {
	iter.iterStack = append(iter.iterStack, i)
	iter.keyStack = append(iter.keyStack, k)
	iter.nodeStack = append(iter.nodeStack, n)
}

func (iter *iterator) Next() bool {
	for currIter, currNode := iter.peek(); currIter != nil; currIter, currNode = iter.peek() {
		if !currIter.Next() {
			if currIter.Error() != nil {
				iter.err = currIter.Error()

				break
			}

			iter.key = iter.keyStack[1:]
			iter.value = currNode.Value
			iter.pop()

			if iter.value != nil && iter.order == kv.SortOrderDesc {
				return true
			}

			continue
		}

		var n compositepb.Node

		if err := n.Unmarshal(currIter.Value()); err != nil {
			iter.err = err

			return false
		}

		if n.ChildRef != 0 {
			nodeNs := keys.Int64ToKey(n.ChildRef)
			nodeIter, err := kv.NamespaceMap(iter.flatMap, nodeNs[:]).Keys(iter.keyRange.Get(len(iter.keyStack)), iter.order)

			if err != nil {
				iter.err = err

				return false
			}

			iter.push(nodeIter, currIter.Key(), n)

			if iter.order != kv.SortOrderDesc && n.Value != nil {
				iter.key = iter.keyStack[1:]
				iter.value = n.Value

				return true
			}
		} else if n.Value != nil {
			iter.key = append(iter.keyStack[1:], currIter.Key())
			iter.value = n.Value
			return true
		}
	}

	return false
}

func (iter *iterator) Key() composite.Key {
	return iter.key
}

func (iter *iterator) Value() []byte {
	return iter.value
}

func (iter *iterator) Error() error {
	return iter.err
}
