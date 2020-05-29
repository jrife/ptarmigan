package kv

import (
	"github.com/jrife/ptarmigan/storage/kv/keys"
)

// NamespaceMapReader returns a namespaced map reader
func NamespaceMapReader(mr MapReader, ns []byte) MapReader {
	return &namespacedMap{ns: ns, reader: mr}
}

// NamespaceMapUpdater returns a namespaced map updater
func NamespaceMapUpdater(mu MapUpdater, ns []byte) MapUpdater {
	return &namespacedMap{ns: ns, updater: mu}
}

// NamespaceMap returns a namespaced map
func NamespaceMap(m Map, ns []byte) Map {
	return &namespacedMap{ns: ns, reader: m, updater: m}
}

// Namespace ensures that all keys referenced within a transaction
// are prefixed with ns.
func Namespace(txn Transaction, ns []byte) Transaction {
	return &namespacedTxn{Map: NamespaceMap(txn, ns), txn: txn}
}

type namespacedMap struct {
	ns      keys.Key
	reader  MapReader
	updater MapUpdater
}

func (nsMap *namespacedMap) key(key keys.Key) keys.Key {
	return keys.All().Namespace(nsMap.ns).Eq(key).Min
}

func (nsMap *namespacedMap) Put(key, value []byte) error {
	return nsMap.updater.Put(nsMap.key(key), value)
}

func (nsMap *namespacedMap) Delete(key []byte) error {
	return nsMap.updater.Delete(nsMap.key(key))
}

func (nsMap *namespacedMap) Get(key []byte) ([]byte, error) {
	return nsMap.reader.Get(nsMap.key(key))
}

func (nsMap *namespacedMap) Keys(keys keys.Range, order SortOrder) (Iterator, error) {
	if order != SortOrderAsc && order != SortOrderDesc {
		order = SortOrderAsc
	}

	iterator, err := nsMap.reader.Keys(keys.Namespace(nsMap.ns), order)

	if err != nil {
		return nil, err
	}

	return &namespacedIterator{iterator: iterator, ns: nsMap.ns}, nil
}

type namespacedIterator struct {
	iterator Iterator
	key      keys.Key
	ns       keys.Key
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

func (nsIter *namespacedIterator) Key() []byte {
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
