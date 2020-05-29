package composite

import (
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
	composite_keys "github.com/jrife/flock/storage/kv/keys/composite"
)

// NamespaceMapReader returns a namespaced map reader
func NamespaceMapReader(mr MapReader, ns [][]byte) MapReader {
	return &namespacedMap{ns: ns, reader: mr}
}

// NamespaceMapUpdater returns a namespaced map updater
func NamespaceMapUpdater(mu MapUpdater, ns [][]byte) MapUpdater {
	return &namespacedMap{ns: ns, updater: mu}
}

// NamespaceMap returns a namespaced map
func NamespaceMap(m Map, ns [][]byte) Map {
	return &namespacedMap{ns: ns, reader: m, updater: m}
}

// Namespace ensures that all keys referenced within a transaction
// are prefixed with ns.
func Namespace(txn Transaction, ns [][]byte) Transaction {
	return &namespacedTxn{Map: NamespaceMap(txn, ns), txn: txn}
}

type namespacedMap struct {
	ns      composite_keys.Key
	reader  MapReader
	updater MapUpdater
}

func (nsMap *namespacedMap) key(key composite_keys.Key) composite_keys.Key {
	k := make(composite_keys.Key, 0, len(nsMap.ns)+len(key))
	k = append(k, nsMap.ns...)
	k = append(k, key...)

	return k
}

func (nsMap *namespacedMap) Put(key composite_keys.Key, value []byte) error {
	return nsMap.updater.Put(nsMap.key(key), value)
}

func (nsMap *namespacedMap) Delete(key composite_keys.Key) error {
	return nsMap.updater.Delete(nsMap.key(key))
}

func (nsMap *namespacedMap) Get(key composite_keys.Key) ([]byte, error) {
	return nsMap.reader.Get(nsMap.key(key))
}

func (nsMap *namespacedMap) Keys(keyRange composite_keys.Range, order kv.SortOrder) (Iterator, error) {
	if order != kv.SortOrderAsc && order != kv.SortOrderDesc {
		order = kv.SortOrderAsc
	}

	nsKeyRange := composite_keys.Range{}

	for _, e := range nsMap.ns {
		nsKeyRange = append(nsKeyRange, keys.All().Eq(e))
	}

	nsKeyRange = append(nsKeyRange, keyRange...)

	iterator, err := nsMap.reader.Keys(nsKeyRange, order)

	if err != nil {
		return nil, err
	}

	return &namespacedIterator{iterator: iterator, ns: nsMap.ns}, nil
}

type namespacedIterator struct {
	iterator Iterator
	key      composite_keys.Key
	ns       composite_keys.Key
}

func (nsIter *namespacedIterator) Next() bool {
	if !nsIter.iterator.Next() {
		nsIter.key = nil

		return false
	}

	// strip the namespace prefix
	nsIter.key = nsIter.iterator.Key()[len(nsIter.ns):]

	return true
}

func (nsIter *namespacedIterator) Key() composite_keys.Key {
	return nsIter.key
}

func (nsIter *namespacedIterator) Value() []byte {
	return nsIter.iterator.Value()
}

func (nsIter *namespacedIterator) Error() error {
	return nsIter.iterator.Error()
}

type namespacedTxn struct {
	Map
	txn Transaction
}

func (nsTxn *namespacedTxn) Metadata() ([]byte, error) {
	return nsTxn.txn.Metadata()
}

func (nsTxn *namespacedTxn) SetMetadata(metadata []byte) error {
	return nsTxn.txn.SetMetadata(metadata)
}

func (nsTxn *namespacedTxn) Commit() error {
	return nsTxn.txn.Commit()
}

func (nsTxn *namespacedTxn) Rollback() error {
	return nsTxn.txn.Rollback()
}
