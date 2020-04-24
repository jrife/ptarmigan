package kv

import (
	"bytes"
	"io"
)

type SortOrder int

const SortOrderAsc SortOrder = 0
const SortOrderDesc SortOrder = 1

type PluginOptions map[string]interface{}

// Plugin represents a kv storage plugin
type Plugin interface {
	// Init initializes the storage plugin
	Init(options PluginOptions) error
	// Name returns the name of the storage plugin
	Name() string
	// NewStore returns an instance of the plugin store
	NewStore(options PluginOptions) (RootStore, error)
	// NewTempStore returns an instance of the plugin store
	// initialized with some sane defaults. It is meant for
	// tests that need an initialized instance of the plugin's
	// store without knowing how to initialize it
	NewTempStore() (RootStore, error)
}

// RootStore
// Store
// Partition
// Key
type RootStore interface {
	Open() error
	Close() error
	Store(name []byte) Store
}

type Store interface {
	Partition(name []byte) Partition
}

type Partition interface {
	Create() error
	Delete() error
	// Begin starts a transaction for the store namespaced
	// to the bucket associated with this SubStore. The
	// transaction will see its root as the current bucket.
	// writable should be true for read-write transactions and
	// false for read-only transactions.
	Begin(writable bool) (Transaction, error)
	// Snapshot takes a consistent snapshot of the bucket
	// associated with this SubStore. If the bucket that
	// this SubStore is namespaced to does not exist then
	// Snapshot will return an error.
	Snapshot() (io.ReadCloser, error)
	// ApplySnapshot applies a snapshot to the bucket
	// associated with this SubStore. Application of
	// a snapshot must preseve the consistency guarantees
	// of the store. In other words, applying a snapshot
	// is just a read-write transaction that completely
	// replaces the contents of the current bucket with
	// the snapshot. Atomicity and isolation guarantees
	// should be preserved. If the bucket that that this
	// SubStore is namespaced to does not exist then
	// ApplySnapshot will return an error.
	ApplySnapshot(io.Reader) error
}

type Transaction interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Keys(min, max []byte, order SortOrder) (Iterator, error)
	Commit() error
	Rollback() error
}

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Error() error
}

type namespacedTxn struct {
	txn Transaction
	ns  []byte
}

func (nsTxn *namespacedTxn) key(key []byte) []byte {
	k := make([]byte, 0, len(nsTxn.ns)+len(key))
	k = append(k, nsTxn.ns...)

	if key != nil {
		k = append(k, key...)
	}

	return k
}

func (nsTxn *namespacedTxn) Put(key, value []byte) error {
	return nsTxn.txn.Put(nsTxn.key(key), value)
}

func (nsTxn *namespacedTxn) Get(key []byte) ([]byte, error) {
	return nsTxn.txn.Get(nsTxn.key(key))
}

func (nsTxn *namespacedTxn) Delete(key []byte) error {
	return nsTxn.txn.Delete(nsTxn.key(key))
}

func (nsTxn *namespacedTxn) Keys(min, max []byte, order SortOrder) (Iterator, error) {
	if order != SortOrderAsc && order != SortOrderDesc {
		order = SortOrderAsc
	}

	if order != SortOrderAsc || max != nil {
		max = nsTxn.key(max)
	}

	if order != SortOrderDesc || min != nil {
		min = nsTxn.key(min)
	}

	iterator, err := nsTxn.Keys(min, max, order)

	if err != nil {
		return nil, err
	}

	return &namespacedIterator{iterator: iterator, ns: nsTxn.ns}, nil
}

func (nsTxn *namespacedTxn) Commit() error {
	return nsTxn.txn.Commit()
}

func (nsTxn *namespacedTxn) Rollback() error {
	return nsTxn.txn.Rollback()
}

type namespacedIterator struct {
	iterator Iterator
	done     bool
	key      []byte
	value    []byte
	ns       []byte
	err      error
}

func (nsCursor *namespacedIterator) Next() bool {
	if nsCursor.done {
		return false
	}

	if !nsCursor.iterator.Next() {
		nsCursor.done = true
		nsCursor.err = nsCursor.iterator.Error()
		return false
	}

	if !bytes.HasPrefix(nsCursor.iterator.Key(), nsCursor.ns) {
		nsCursor.done = true
		return false
	}

	// strip the namespace prefix
	nsCursor.key = nsCursor.iterator.Key()[len(nsCursor.ns):]
	nsCursor.value = nsCursor.iterator.Value()

	return true
}

func (nsCursor *namespacedIterator) Key() []byte {
	if nsCursor.done {
		return nil
	}

	return nsCursor.key
}

func (nsCursor *namespacedIterator) Value() []byte {
	if nsCursor.done {
		return nil
	}

	return nsCursor.value
}

func (nsCursor *namespacedIterator) Error() error {
	return nsCursor.err
}

func Namespace(txn Transaction, ns []byte) Transaction {
	return &namespacedTxn{txn: txn, ns: ns}
}
