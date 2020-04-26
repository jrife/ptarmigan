package kv

import (
	"bytes"
	"errors"
	"io"
)

var (
	// ErrClosed indicates that the root store was closed
	ErrClosed = errors.New("root store was closed")
	// ErrNoSuchStore indicates that the store doesn't exist. Either it hasn't been created or was deleted
	ErrNoSuchStore = errors.New("store does not exist")
	// ErrNoSuchPartition indicates that the partition doesn't exist. Either it hasn't been created or was deleted
	ErrNoSuchPartition = errors.New("partition does not exist")
)

// SortOrder describes sort order for iteration
// Either SortOrderAsc or SortOrderDesc
type SortOrder int

// SortOrderAsc sorts in increasing order
const SortOrderAsc SortOrder = 0

// SortOrderDesc sorts in decreasing order
const SortOrderDesc SortOrder = 1

// PluginOptions is a generic structure to pass
// arbitrary configuration to a storage plugin
type PluginOptions map[string]interface{}

// Plugin represents a kv storage plugin
type Plugin interface {
	// Init initializes the storage plugin
	Init(options PluginOptions) error
	// Name returns the name of the storage plugin
	Name() string
	// NewStore returns an instance of the plugin store
	NewRootStore(options PluginOptions) (RootStore, error)
	// NewTempStore returns an instance of the plugin store
	// initialized with some sane defaults. It is meant for
	// tests that need an initialized instance of the plugin's
	// store without knowing how to initialize it
	NewTempRootStore() (RootStore, error)
}

// RootStore is the parent store from which all stores are descended
type RootStore interface {
	// Delete closes then deletes this store and all its contents.
	// If the root store doesn't exist it should return nil and have
	// no effect.
	Delete() error
	// Close closes the store. Function calls to any I/O objects
	// descended from this store occurring after Close returns
	// must have no effect and return ErrClosed. Close must not
	// return until all concurrent I/O operations have concluded
	// and all transactions have either rolled back or committed.
	// Operations started after the call to Close is started but
	// before it returns may proceed normally or may return ErrClosed.
	// If they return ErrClosed they must have no effect. Close may
	// return an error to indicate any problems that occurred during
	// shutdown. Simply put, when close returns it should ensure the caller
	// that the state of all the stores is fixed and will not change unless
	// this root store is reopened
	Close() error
	// Stores lists all the stores inside this root store. Results must
	// be lexicographically ordered by store name. It must return
	// ErrClosed if its invocation starts after Close() returns.
	Stores() ([][]byte, error)
	// Store returns a handle for the store with this name. It does not
	// guarantee that this store exists yet and should not create the
	// store. It must not return nil.
	Store(name []byte) Store
}

// Store is a reference to a working store
type Store interface {
	// Name returns the name of this store
	Name() []byte
	// Create creates this store if it does not exist. It has no
	// effect if the store already exists. It must return ErrClosed
	// if its invocation starts after Close() on the root store returns
	Create() error
	// Delete deletes this store if it exists. It has no effect
	// if the store does not exist. It must return ErrClosed if
	// its invocation starts after Close() on the root store returns.
	// Delete must not return until all concurrent I/O operations have
	// concluded and all transactions have either rolled back or committed.
	Delete() error
	// Partitions lists up to limit partitions in this store whose name
	// is in the range [min, max). List results must be lexocographically
	// ordered and contiguous. In other words, if partitions A, B, and C
	// exist and a consumer calls Partitions([]byte("A"), []byte("C"), 2)
	// the return value must be [][]byte{[]byte("A"), []byte("B")} rather
	// than [][]byte{[]byte("A"), []byte("C")}. There must be no gaps. A
	// caller making subsequent calls to Partitions() may observe inconsistencies
	// if Partitions() are created and deleted between calls to Partitions(). It
	// is up to the caller to control the ordering between creation, deletion,
	// and listing of partitions if their use case demands it. Partitions must
	// return ErrClosed if its invocation starts after Close() on the root store
	// returns. Otherwise it must return ErrNoSuchStore if this store does not exist.
	// min = nil indicates the lexocographically lowest partition name.
	// max = nil indicates the lexocographically highest partition name. limit < 0
	// indicates no limit.
	Partitions(min, max []byte, limit int) ([][]byte, error)
	// Partition returns a handle for the partition with this name inside this store.
	// It does not guarantee that this partition exists yet and should not create the partition.
	// It must not return nil.
	Partition(name []byte) Partition
}

// Partition is a reference to a named partition of a store.
// Strict-serializability must be enforced on all transactions
// within a partition. Partitions are more or less independent, however,
// and do not require coordination between them.
// Serializability: transactions appear to occur in some total order
// Strict: from linearizability, transaction ordering respects real-time ordering
// Simply put, strict serializability implies that a transaction that begins
// after another transaction ends shall observe the effects of the first transaction
// Strict-serializability is basically the default mode for many kv drivers that
// employ pessemistic concurrency control, although many just call it "serializable".
// I want to make it clear here that transactions must also respect the real-time
// ordering constraints implied by "strict" as consumers will make the assumption that
// this is the case (such as expecting no stale reads). This interface does not
// prescribe optimistic or pessemistic concurrency control. However, the decision between
// pessimistic vs optimistic concurrency control does carry with it implications on how
// the interface should be consumed (such as a consumer retrying transactions under certain
// conditions or requiring ordering of transaction operations so as to avoid resource contention
// between two concurrent transactions). For now we will assume drivers use pessimistic concurrency
// control, but to make any future transition easier to a driver that uses optimistic concurrency
// control consumers should try not to rely on the assumption that calls to Begin() ensure mutual exclusion
// and should perform their own locking to control concurrency for their own data. However, consumers
// should assume that Begin() may block and appropriately order calls to locks within their own applications.
//
// Don't Do This (Possible Deadlock):
// Thread A:
//   1) a.Lock()
//   2) p.Begin(true)
// Thread B:
//   1) p.Begin(true)
//   2) a.Lock()
//
// Do This
// Thread A:
//   1) a.Lock()
//   2) p.Begin(true)
// Thread B:
//   1) a.Lock()
//   2) p.Begin(true)
type Partition interface {
	// Name returns the name of this partition
	Name() []byte
	// Create creates this partition if it does not exist. It has no
	// effect if the partition already exists. It must return ErrClosed
	// if its invocation starts after Close() on the root store returns.
	// Otherwise it must return ErrNoSuchStore if the parent store does not exist.
	Create() error
	// Delete deletes this partition if it exists. It has no effect if
	// the partition does not exist. It must return ErrClosed if its
	// invocation starts after Close() on the root store returns. Otherwise it
	// must return ErrNoSuchStore if the parent store does not exist.
	Delete() error
	// Begin starts a transaction for this partition. writable should be
	// true for read-write transactions and false for read-only transactions.
	// If Begin() is called after Close() on the root store returns it must
	// return ErrClosed. Otherwise if the parent store does not exist it must
	// return ErrNoSuchStore. Otherwise if this partition does not exist it must
	// return ErrNoSuchPartition.
	Begin(writable bool) (Transaction, error)
	// Snapshot takes a consistent snapshot of this partition. If Snapshot() is called
	// after Close() on the root store returns it must return ErrClosed. Otherwise if
	// the parent store does not exist it must return ErrNoSuchStore. Otherwise if this
	// partition does not exist it must return ErrNoSuchPartition. Snapshot is like a
	// self-contained read-only transaction. Until all calls to Snapshot() return
	// Close() on the root store must not return and must wait for all snapshots
	// to finish being read or closed. Calls to Snapshot() started after Close() is called
	// may return ErrClosed right away. Likewise, the snapshot should be consistent:
	// serializability must be maintained and its view must be consistent with the
	// most recently commited read-write transaction. Strict-serializability must
	// be enforced.
	Snapshot() (io.ReadCloser, error)
	// ApplySnapshot applies a snapshot to this partition. If ApplySnapshot() is called
	// after Close() on the root store returns it must return ErrClosed. Otherwise if
	// the parent store does not exist it must return ErrNoSuchStore. If this partition
	// doesn't exist ApplySnapshot will create it. If the partition does exist ApplySnapshot
	// overwrites the state currently stored in the partition. ApplySnapshot
	// is like a self-contained read-write transaction. Until all calls to ApplySnapshot()
	// return Close() on the root store must not return. Calls to ApplySnapshot() started
	// after Close() is called may return ErrClosed right away. Strict-serializability must
	// be enforced.
	ApplySnapshot(io.Reader) error
}

// Transaction is a transaction for a partition. It must only be
// used by one goroutine at a time.
type Transaction interface {
	// Put puts a key. Put must return an error
	// if either key or value is nil or empty.
	Put(key, value []byte) error
	// Get gets a key. It must observe updates to that key made
	// previously by this transation. Get must return an error
	// if the key is nil or empty. It must return nil if the
	// requested key does not exist.
	Get(key []byte) ([]byte, error)
	// Delete deletes a key. It must return an error if the key
	// is nil or empty. If the key doesn't exist it has no effect
	// and returns nil.
	Delete(key []byte) error
	// Keys creates an iterator that iterates over the range
	// of keys specified by the
	Keys(min, max []byte, order SortOrder) (Iterator, error)
	// Commit commits the transaction
	Commit() error
	// Rollback rolls back the transaction
	Rollback() error
}

// Iterator iterates over a set of keys. It must only be
// used by one goroutine at a time. Consumers should not
// attempt to use an iterator once its parent transaction
// has been rolled back. Behavior is undefined in this case.
// The transaction must not mutate the store when the iterator
// is in use. This may cause inconsistent behavior.
type Iterator interface {
	// Next advances the iterator to the next key
	// A fresh iterator must call Next once to
	// advance to the first key. Next returns false
	// if there is no next key or if it encounters an
	// error.
	Next() bool
	// Key returns the current key
	Key() []byte
	// Value returns the current value
	Value() []byte
	// Error returns the error, if any.
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

	iterator, err := nsTxn.txn.Keys(min, max, order)

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

// Namespace ensures that all keys referenced within a transaction
// are prefixed with the ns prefix. Can be chained.
func Namespace(txn Transaction, ns []byte) Transaction {
	return &namespacedTxn{txn: txn, ns: ns}
}
