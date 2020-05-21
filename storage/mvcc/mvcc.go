package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/keys"
)

const (
	// RevisionOldest can be used in place of a revision
	// number to access the oldest revision
	RevisionOldest int64 = -1
	// RevisionNewest can be used in place of a revision
	// number to access the neweset revision
	RevisionNewest int64 = 0
)

var (
	keysPrefix      = []byte{1}
	revisionsPrefix = []byte{2}
)

// b must be a byte slice of length 8
func bytesToInt64(b []byte) int64 {
	if len(b) != 8 {
		panic("b must be length 8")
	}

	return int64(binary.BigEndian.Uint64(b))
}

// b must be a byte slice of length 8
// n must be >= 0
func int64ToBytes(b []byte, n int64) {
	if len(b) != 8 {
		panic("b must be length 8")
	}

	if n < 0 {
		panic("n must be positive")
	}

	binary.BigEndian.PutUint64(b, uint64(n))
}

type revisionsKey []byte

func newRevisionsKey(revision int64, key []byte) revisionsKey {
	if key == nil {
		b := make([]byte, 8)
		int64ToBytes(b, revision)

		return b
	}

	b := make([]byte, len(key)+9)
	int64ToBytes(b[:8], revision)
	b[8] = 0
	copy(b[9:], key)

	return b
}

func (k revisionsKey) revision() (int64, error) {
	if len(k) < 8 {
		return 0, fmt.Errorf("buffer is not long enough to contain a revision number")
	}

	return bytesToInt64(k[:8]), nil
}

func (k revisionsKey) key() ([]byte, error) {
	if len(k) < 9 {
		return nil, nil
	}

	return k[9:], nil
}

type revisionsValue []byte

func newRevisionsValue(value []byte) []byte {
	if value == nil {
		return []byte{}
	}

	v := make([]byte, len(value)+1)
	v[0] = 0
	copy(v[1:], value)

	return v
}

func (v revisionsValue) value() []byte {
	if len(v) == 0 {
		return nil
	}

	return v[1:]
}

type keysKey []byte

func newKeysKey(key []byte, revision int64) keysKey {
	if revision <= 0 {
		return key
	}

	b := make([]byte, len(key)+9)
	int64ToBytes(b[len(key)+1:], revision)
	b[len(key)] = 0
	copy(b[:len(key)], key)

	return b
}

func (k keysKey) revision() (int64, error) {
	if len(k) < 9 {
		return 0, fmt.Errorf("buffer is not long enough to contain a revision number")
	}

	return bytesToInt64(k[len(k)-8:]), nil
}

func (k keysKey) key() ([]byte, error) {
	if len(k) < 9 {
		return nil, fmt.Errorf("buffer is not long enough to contain a key")
	}

	return k[:len(k)-9], nil
}

type keysValue []byte

func newKeysValue(value []byte) []byte {
	if value == nil {
		return []byte{}
	}

	v := make([]byte, len(value)+1)
	v[0] = 0
	copy(v[1:], value)

	return v
}

func (v keysValue) value() []byte {
	if len(v) == 0 {
		return nil
	}

	return v[1:]
}

// New creates a new mvcc store
func New(kvStore kv.Store) (Store, error) {
	store := &store{
		kvStore: kvStore,
	}

	// Always ensure that the store exists
	if err := kvStore.Create(); err != nil {
		return nil, wrapError("could not ensure store exists", err)
	}

	return store, nil
}

var _ Store = (*store)(nil)

type store struct {
	kvStore kv.Store
	close   sync.Once
	closed  struct {
		sync.RWMutex
		closed bool
	}
}

// Close implements Store.Close
func (store *store) Close() error {
	store.close.Do(func() {
		store.closed.Lock()
		defer store.closed.Unlock()

		store.closed.closed = true
	})

	return nil
}

// Close implements Store.Delete
func (store *store) Delete() error {
	store.Close()

	return wrapError("could not delete store", store.kvStore.Delete())
}

// Partitions implements Store.Partitions
func (store *store) Partitions(nameRange keys.Range, limit int) ([][]byte, error) {
	store.closed.RLock()
	defer store.closed.RUnlock()

	if store.closed.closed {
		return nil, ErrClosed
	}

	names, err := store.kvStore.Partitions(nameRange, limit)

	return names, wrapError("could not list partitions", err)
}

// Partition implements Store.Partition
func (store *store) Partition(name []byte) Partition {
	return &partition{store: store, name: name}
}

type partition struct {
	store *store
	name  []byte
}

func (partition *partition) beginTxn(writable bool) (kv.Transaction, error) {
	return partition.store.kvStore.Partition(partition.name).Begin(writable)
}

func (partition *partition) newestRevision(transaction kv.Transaction) (int64, error) {
	iter, err := newRevisionsIterator(partition.revisionsNamespace(transaction), nil, nil, kv.SortOrderDesc)

	if err != nil {
		return 0, wrapError("could not create iterator", err)
	}

	if !iter.next() {
		if iter.error() != nil {
			return 0, wrapError("iteration error", iter.error())
		}

		return 0, nil
	}

	return iter.revision(), nil
}

func (partition *partition) oldestRevision(transaction kv.Transaction) (int64, error) {
	iter, err := newRevisionsIterator(partition.revisionsNamespace(transaction), nil, nil, kv.SortOrderAsc)

	if err != nil {
		return 0, wrapError("could not create iterator", err)
	}

	if !iter.next() {
		if iter.error() != nil {
			return 0, wrapError("iteration error", iter.error())
		}

		return 0, nil
	}

	return iter.revision(), nil
}

func (partition *partition) nextRevision(transaction kv.Transaction) (int64, error) {
	lastRevision, err := partition.newestRevision(transaction)

	if err != nil {
		return 0, err
	}

	nextRevision := lastRevision + 1

	if nextRevision < 0 {
		return 0, fmt.Errorf("rollover detected, this should not happen")
	}

	return nextRevision, nil
}

func (partition *partition) revisionsNamespace(transaction kv.Transaction) kv.Transaction {
	return kv.Namespace(transaction, revisionsPrefix)
}

func (partition *partition) keysNamespace(transaction kv.Transaction) kv.Transaction {
	return kv.Namespace(transaction, keysPrefix)
}

// Name implements Partition.Name
func (partition *partition) Name() []byte {
	return partition.name
}

// Create implements Partition.Create
func (partition *partition) Create(metadata []byte) error {
	partition.store.closed.RLock()
	defer partition.store.closed.RUnlock()

	if partition.store.closed.closed {
		return ErrClosed
	}

	if err := partition.store.kvStore.Partition(partition.name).Create(metadata); err != nil {
		return wrapError("could not create partition", err)
	}

	return nil
}

// Delete implements Partition.Delete
func (partition *partition) Delete() error {
	partition.store.closed.RLock()
	defer partition.store.closed.RUnlock()

	if partition.store.closed.closed {
		return ErrClosed
	}

	if err := partition.store.kvStore.Partition(partition.name).Delete(); err != nil {
		return wrapError("could not delete partition", err)
	}

	return nil
}

// Metadata implements Partition.Metadata
func (partition *partition) Metadata() ([]byte, error) {
	partition.store.closed.RLock()
	defer partition.store.closed.RUnlock()

	if partition.store.closed.closed {
		return nil, ErrClosed
	}

	transaction, err := partition.beginTxn(false)

	if err != nil {
		return nil, wrapError("could not begin transaction", err)
	}

	defer transaction.Rollback()

	metadata, err := transaction.Metadata()

	return metadata, wrapError("could not retrieve metadata from transaction", err)
}

// Transaction implements Partition.Transaction
func (partition *partition) Begin(writable bool) (Transaction, error) {
	partition.store.closed.RLock()

	if partition.store.closed.closed {
		partition.store.closed.RUnlock()

		return nil, ErrClosed
	}

	txn, err := partition.beginTxn(writable)

	if err != nil {
		partition.store.closed.RUnlock()

		return nil, wrapError("could not begin transaction", err)
	}

	return &transaction{partition: partition, txn: txn, writable: writable}, nil
}

// ApplySnapshot implements Partition.ApplySnapshot
func (partition *partition) ApplySnapshot(snap io.Reader) error {
	partition.store.closed.RLock()
	defer partition.store.closed.RUnlock()

	if partition.store.closed.closed {
		return ErrClosed
	}

	return wrapError("could not apply kv store snapshot", partition.store.kvStore.Partition(partition.name).ApplySnapshot(snap))
}

// Snapshot implements Partition.Snapshot
func (partition *partition) Snapshot() (io.ReadCloser, error) {
	partition.store.closed.RLock()
	defer partition.store.closed.RUnlock()

	if partition.store.closed.closed {
		return nil, ErrClosed
	}

	snap, err := partition.store.kvStore.Partition(partition.name).Snapshot()

	return snap, wrapError("could not take kv store snapshot", err)
}

type transaction struct {
	partition *partition
	txn       kv.Transaction
	revision  Revision
	writable  bool
	close     sync.Once
}

// NewRevision implements Transaction.NewRevision
func (transaction *transaction) NewRevision() (Revision, error) {
	if transaction.revision != nil {
		return nil, ErrTooManyRevisions
	}

	if !transaction.writable {
		return nil, ErrReadOnly
	}

	nextRevision, err := transaction.partition.nextRevision(transaction.txn)

	if err != nil {
		return nil, wrapError("could not calculate next revision number", err)
	}

	revisionsTxn := transaction.partition.revisionsNamespace(transaction.txn)

	if err := revisionsTxn.Put(newRevisionsKey(nextRevision, nil), []byte{}); err != nil {
		return nil, wrapError("could not insert revision start market", err)
	}

	r := &revision{
		view: view{
			partition: transaction.partition,
			txn:       transaction.txn,
			revision:  nextRevision,
		},
	}

	transaction.revision = r

	return r, nil
}

// View implements Transaction.View
func (transaction *transaction) View(revision int64) (View, error) {
	if transaction.partition.store.closed.closed {
		return nil, ErrClosed
	}

	newestRevision, err := transaction.partition.newestRevision(transaction.txn)

	if err != nil {
		return nil, wrapError("unable to retrieve current revision", err)
	}

	oldestRevision, err := transaction.partition.oldestRevision(transaction.txn)

	if err != nil {
		return nil, wrapError("unable to retrieve oldest revision", err)
	}

	// it's possible that both newestRevision
	// and oldestRevision are 0. In such cases
	// this indicates that this partition is new
	// and has not had any revisions written to
	// it yet.
	if newestRevision == 0 && oldestRevision == 0 {
		return nil, ErrNoRevisions
	}

	if revision == RevisionNewest {
		revision = newestRevision
	} else if revision <= RevisionOldest {
		revision = oldestRevision
	}

	if revision > newestRevision {
		return nil, ErrRevisionTooHigh
	} else if revision < oldestRevision {
		return nil, ErrCompacted
	}

	return &view{partition: transaction.partition, revision: revision, txn: transaction.txn}, nil
}

// Compact implements Transaction.Compact
func (transaction *transaction) Compact(revision int64) error {
	if !transaction.writable {
		return ErrReadOnly
	}

	newestRevision, err := transaction.partition.newestRevision(transaction.txn)

	if err != nil {
		return wrapError("unable to retrieve current revision", err)
	}

	oldestRevision, err := transaction.partition.oldestRevision(transaction.txn)

	if err != nil {
		return wrapError("unable to retrieve oldest revision", err)
	}

	// it's possible that both newestRevision
	// and oldestRevision are 0. In such cases
	// this indicates that this partition is new
	// and has not had any revisions written to
	// it yet.
	if newestRevision == 0 && oldestRevision == 0 {
		return ErrNoRevisions
	}

	if revision == RevisionNewest {
		revision = newestRevision
	} else if revision <= RevisionOldest {
		revision = oldestRevision
	}

	if revision > newestRevision {
		return ErrRevisionTooHigh
	} else if revision < oldestRevision {
		return ErrCompacted
	}

	keysNs, revsNs, errors := transaction.compactKeys(revision)
	keysTxn := transaction.partition.keysNamespace(transaction.txn)
	revsTxn := transaction.partition.revisionsNamespace(transaction.txn)

	for {
		select {
		case key := <-keysNs:
			keysTxn.Delete(key)
		case key := <-revsNs:
			revsTxn.Delete(key)
		case err := <-errors:
			return err
		}
	}
}

func (transaction *transaction) compactKeys(revision int64) (<-chan []byte, <-chan []byte, <-chan error) {
	keysNs := make(chan []byte)
	revsNs := make(chan []byte)
	errors := make(chan error, 1)

	go func() {
		txn, err := transaction.partition.beginTxn(false)

		if err != nil {
			errors <- wrapError("could not begin transaction", err)

			return
		}

		defer txn.Rollback()

		revsIter, err := newRevisionsIterator(transaction.partition.revisionsNamespace(txn), nil, &revisionsCursor{revision: revision}, kv.SortOrderAsc)

		if err != nil {
			errors <- wrapError("could not create revisions iterator", err)

			return
		}

		keysIter, err := newKeysIterator(transaction.partition.keysNamespace(txn), nil, nil, kv.SortOrderDesc)

		if err != nil {
			errors <- wrapError("could not create keys iterator", err)

			return
		}

		for revsIter.next() {
			revsNs <- newRevisionsKey(revsIter.revision(), revsIter.key())
		}

		if revsIter.error() != nil {
			errors <- revsIter.error()

			return
		}

		for prevKey, prevRev := []byte(nil), int64(0); keysIter.next(); prevKey, prevRev = keysIter.key(), keysIter.revision() {
			if keysIter.revision() >= revision {
				continue
			}

			// Keep only the newest version of each key as of the compact revision.
			// Always compact tombstones
			if keysIter.value() == nil || bytes.Compare(prevKey, keysIter.key()) == 0 && prevRev <= revision {
				keysNs <- newKeysKey(keysIter.key(), keysIter.revision())
			}
		}

		if keysIter.error() != nil {
			errors <- keysIter.error()

			return
		}

		errors <- nil
	}()

	return keysNs, revsNs, errors
}

// Commit implements Transaction.Commit
func (transaction *transaction) Commit() error {
	defer transaction.close.Do(transaction.partition.store.closed.RUnlock)

	return transaction.txn.Commit()
}

// Rollback implements Transaction.Rollback
func (transaction *transaction) Rollback() error {
	defer transaction.close.Do(transaction.partition.store.closed.RUnlock)

	return transaction.txn.Rollback()
}

type revision struct {
	view
}

// Put implements Revision.Put
func (revision *revision) Put(key []byte, value []byte) error {
	if err := revision.partition.revisionsNamespace(revision.txn).Put(newRevisionsKey(revision.revision, key), newRevisionsValue(value)); err != nil {
		return err
	}

	return revision.partition.keysNamespace(revision.txn).Put(newKeysKey(key, revision.revision), newKeysValue(value))
}

// Delete implements Revision.Delete
func (revision *revision) Delete(key []byte) error {
	v, err := revision.Get(key)

	if err != nil {
		return wrapError("could not get key", err)
	}

	if v == nil {
		return nil
	}

	if err := revision.partition.revisionsNamespace(revision.txn).Put(newRevisionsKey(revision.revision, key), newRevisionsValue(nil)); err != nil {
		return err
	}

	return revision.partition.keysNamespace(revision.txn).Put(newKeysKey(key, revision.revision), newKeysValue(nil))
}

type view struct {
	revision  int64
	partition *partition
	txn       kv.Transaction
	close     sync.Once
}

// Get implements View.Get
func (view *view) Get(key []byte) ([]byte, error) {
	iter, err := view.Keys(keys.All().Eq(key), kv.SortOrderAsc)

	if err != nil {
		return nil, err
	}

	if !iter.Next() {
		return nil, iter.Error()
	}

	return iter.Value(), nil
}

// Keys implements View.Keys
func (view *view) Keys(keys keys.Range, order kv.SortOrder) (kv.Iterator, error) {
	iter, err := newViewRevisionsIterator(view.partition.keysNamespace(view.txn), keys.Min, keys.Max, view.revision, order)

	if err != nil {
		return nil, wrapError("could not create iterator", err)
	}

	return iter, nil
}

// Changes implements View.Changes
func (view *view) Changes(keys keys.Range, includePrev bool) (DiffIterator, error) {
	iter, err := newViewRevisionDiffsIterator(view.partition.revisionsNamespace(view.txn), keys.Min, keys.Max, view.revision)

	if err != nil {
		return nil, wrapError("could not create diff iterator", err)
	}

	return iter, nil
}

// Revision implements View.Revision
func (view *view) Revision() int64 {
	return view.revision
}
