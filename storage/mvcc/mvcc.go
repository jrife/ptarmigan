package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

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
		return nil, fmt.Errorf("could not ensure store exists: %s", err)
	}

	return store, nil
}

var _ Store = (*store)(nil)

type store struct {
	kvStore kv.Store
}

// Close implements Store.Close
func (store *store) Close() error {
	return nil
}

// Close implements Store.Delete
func (store *store) Delete() error {
	return nil
}

// Partitions implements Store.Partitions
func (store *store) Partitions(nameRange keys.Range, limit int) ([][]byte, error) {
	names, err := store.kvStore.Partitions(nameRange, limit)

	return names, fromKVError(err)
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
		return 0, fmt.Errorf("could not create iterator: %s", err)
	}

	if !iter.next() {
		if iter.error() != nil {
			return 0, fmt.Errorf("iteration error: %s", iter.error().Error())
		}

		return 0, nil
	}

	return iter.revision(), nil
}

func (partition *partition) oldestRevision(transaction kv.Transaction) (int64, error) {
	iter, err := newRevisionsIterator(partition.revisionsNamespace(transaction), nil, nil, kv.SortOrderAsc)

	if err != nil {
		return 0, fmt.Errorf("could not create iterator: %s", err)
	}

	if !iter.next() {
		if iter.error() != nil {
			return 0, fmt.Errorf("iteration error: %s", iter.error().Error())
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
	if err := partition.store.kvStore.Partition(partition.name).Create(metadata); err != nil {
		return fmt.Errorf("could not create partition: %s", err)
	}

	return nil
}

// Delete implements Partition.Delete
func (partition *partition) Delete() error {
	if err := partition.store.kvStore.Partition(partition.name).Delete(); err != nil {
		return fmt.Errorf("could not delete partition: %s", err)
	}

	return nil
}

// Metadata implements Partition.Metadata
func (partition *partition) Metadata() ([]byte, error) {
	transaction, err := partition.beginTxn(false)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %s", err)
	}

	defer transaction.Rollback()

	return transaction.Metadata()
}

// Transaction implements Partition.Transaction
func (partition *partition) Begin() (Transaction, error) {
	txn, err := partition.beginTxn(true)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %s", err)
	}

	return &transaction{partition: partition, txn: txn}, nil
}

// View implements Partition.View
func (partition *partition) View(revision int64) (View, error) {
	txn, err := partition.beginTxn(false)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %s", err)
	}

	newestRevision, err := partition.newestRevision(txn)

	if err != nil {
		txn.Rollback()

		return nil, fmt.Errorf("unable to retrieve current revision: %s", err)
	}

	oldestRevision, err := partition.oldestRevision(txn)

	if err != nil {
		txn.Rollback()

		return nil, fmt.Errorf("unable to retrieve oldest revision: %s", err)
	}

	if revision == RevisionNewest {
		revision = newestRevision
	} else if revision <= RevisionOldest {
		revision = oldestRevision
	}

	// it's possible that both newestRevision
	// and oldestRevision are 0. In such cases
	// this indicates that this partition is new
	// and has not had any revisions written to
	// it yet.
	if revision == 0 {
		txn.Rollback()

		return nil, ErrNoRevisions
	}

	if revision > newestRevision {
		txn.Rollback()

		return nil, ErrRevisionTooHigh
	} else if revision < oldestRevision {
		txn.Rollback()

		return nil, ErrCompacted
	}

	return &view{partition: partition, revision: revision, txn: txn}, nil
}

// ApplySnapshot implements Partition.ApplySnapshot
func (partition *partition) ApplySnapshot(snap io.Reader) error {
	return partition.store.kvStore.Partition(partition.name).ApplySnapshot(snap)
}

// Snapshot implements Partition.Snapshot
func (partition *partition) Snapshot() (io.Reader, error) {
	return partition.store.kvStore.Partition(partition.name).Snapshot()
}

type transaction struct {
	partition *partition
	txn       kv.Transaction
}

// NewRevision implements Transaction.NewRevision
func (transaction *transaction) NewRevision() (Revision, error) {
	nextRevision, err := transaction.partition.nextRevision(transaction.txn)

	if err != nil {
		return nil, fmt.Errorf("could not calculate next revision number: %s", err)
	}

	revisionsTxn := transaction.partition.revisionsNamespace(transaction.txn)

	if err := revisionsTxn.Put(newRevisionsKey(nextRevision, nil), []byte{}); err != nil {
		return nil, fmt.Errorf("could not insert revision start market: %s", err)
	}

	return &revision{
		view: view{
			partition: transaction.partition,
			txn:       transaction.txn,
			revision:  nextRevision,
		},
	}, nil
}

// Compact implements Transaction.Compact
func (transaction *transaction) Compact(revision int64) error {
	if revision <= 1 {
		return nil
	}

	newestRevision, err := transaction.partition.newestRevision(transaction.txn)

	if err != nil {
		return fmt.Errorf("unable to retrieve current revision: %s", err)
	}

	oldestRevision, err := transaction.partition.oldestRevision(transaction.txn)

	if err != nil {
		return fmt.Errorf("unable to retrieve oldest revision: %s", err)
	}

	if revision == RevisionNewest {
		revision = newestRevision
	} else if revision <= RevisionOldest {
		revision = oldestRevision
	}

	// it's possible that both newestRevision
	// and oldestRevision are 0. In such cases
	// this indicates that this partition is new
	// and has not had any revisions written to
	// it yet.
	if revision == 0 {
		return ErrNoRevisions
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
			errors <- fmt.Errorf("could not begin transaction: %s", err)

			return
		}

		defer txn.Rollback()

		revsIter, err := newRevisionsIterator(transaction.partition.revisionsNamespace(txn), nil, &revisionsCursor{revision: revision - 1}, kv.SortOrderAsc)

		if err != nil {
			errors <- fmt.Errorf("could not create revisions iterator: %s", err)

			return
		}

		keysIter, err := newKeysIterator(transaction.partition.keysNamespace(txn), nil, nil, kv.SortOrderDesc)

		if err != nil {
			errors <- fmt.Errorf("could not create keys iterator: %s", err)

			return
		}

		for revsIter.next() {
			revsNs <- newRevisionsKey(revsIter.revision(), revsIter.key())
		}

		for prevKey := []byte(nil); keysIter.next(); prevKey = keysIter.key() {
			// Delete any key whose revision is < revision
			// as long as it's not the newest revision for
			// that key and not a tombstone.
			if bytes.Compare(prevKey, keysIter.key()) == 0 || keysIter.value() == nil {
				keysNs <- newKeysKey(keysIter.key(), keysIter.revision())
			}
		}

		errors <- nil
	}()

	return keysNs, revsNs, errors
}

// Commit implements Transaction.Commit
func (transaction *transaction) Commit() error {
	return transaction.txn.Commit()
}

// Rollback implements Transaction.Rollback
func (transaction *transaction) Rollback() error {
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
	if err := revision.partition.revisionsNamespace(revision.txn).Put(newRevisionsKey(revision.revision, key), newRevisionsValue(nil)); err != nil {
		return err
	}

	return revision.partition.keysNamespace(revision.txn).Put(newKeysKey(key, revision.revision), newKeysValue(nil))
}

type view struct {
	revision  int64
	partition *partition
	txn       kv.Transaction
}

// Get implements View.Get
func (view *view) Get(key []byte) ([]byte, error) {
	return view.partition.keysNamespace(view.txn).Get(key)
}

// Keys implements View.Keys
func (view *view) Keys(keys keys.Range, order kv.SortOrder) (kv.Iterator, error) {
	iter, err := newViewRevisionsIterator(view.partition.keysNamespace(view.txn), keys.Min, keys.Max, view.revision, order)

	if err != nil {
		return nil, fmt.Errorf("could not create iterator: %s", err)
	}

	return iter, nil
}

// Changes implements View.Changes
func (view *view) Changes(keys keys.Range, includePrev bool) (DiffIterator, error) {
	iter, err := newViewRevisionDiffsIterator(view.partition.revisionsNamespace(view.txn), keys.Min, keys.Max, view.revision)

	if err != nil {
		return nil, fmt.Errorf("could not create diff iterator: %s", err)
	}

	return iter, nil
}

// Revision implements View.Revision
func (view *view) Revision() int64 {
	return view.revision
}

// Close implements View.Close
func (view *view) Close() error {
	return view.txn.Rollback()
}
