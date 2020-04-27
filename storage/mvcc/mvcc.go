package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jrife/ptarmigan/storage/kv"
)

var (
	metadataPrefix  = []byte{0}
	keysPrefix      = []byte{1}
	revisionsPrefix = []byte{2}
	metadataKey     = []byte{0}
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
		return nil, fmt.Errorf("buffer is not long enough to contain a key")
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

	return bytesToInt64(k[len(k)-9:]), nil
}

func (k keysKey) key() ([]byte, error) {
	if len(k) < 9 {
		return nil, fmt.Errorf("buffer is not long enough to contain a key")
	}

	return k[0 : len(k)-9], nil
}

type keysValue []byte

// New creates a new mvcc store
func New(kvStore kv.Store) Store {
	store := &store{
		kvStore: kvStore,
	}

	return store
}

var _ Store = (*store)(nil)

type store struct {
	kvStore kv.Store
}

// Open implements Store.Open
func (store *store) Open() error {
	// Always ensure that the store exists
	if err := store.kvStore.Create(); err != nil {
		return fmt.Errorf("could not ensure store exists: %s", err.Error())
	}

	return nil
}

// Close implements Store.Close
func (store *store) Close() error {
	return nil
}

// Partitions implements Store.Partitions
func (store *store) Partitions(min []byte, max []byte, limit int) ([][]byte, error) {
	return store.kvStore.Partitions(min, max, limit)
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
	iter, err := newRevisionsIterator(partition.revisionsNamespace(transaction), nil, nil, SortOrderDesc)

	if err != nil {
		return 0, fmt.Errorf("could not create iterator: %s", err.Error())
	}

	if !iter.next() {
		if iter.error() != nil {
			return 0, fmt.Errorf("iteration error: %s", err.Error())
		}

		return 0, nil
	}

	return iter.revision(), nil
}

func (partition *partition) oldestRevision(transaction kv.Transaction) (int64, error) {
	iter, err := newRevisionsIterator(partition.revisionsNamespace(transaction), nil, nil, SortOrderAsc)

	if err != nil {
		return 0, fmt.Errorf("could not create iterator: %s", err.Error())
	}

	if !iter.next() {
		if iter.error() != nil {
			return 0, fmt.Errorf("iteration error: %s", err.Error())
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

func (partition *partition) metadataNamespace(transaction kv.Transaction) kv.Transaction {
	return kv.Namespace(transaction, metadataPrefix)
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
	if err := partition.store.kvStore.Partition(partition.name).Create(); err != nil {
		return fmt.Errorf("could not create partition: %s", err.Error())
	}

	return nil
}

// Delete implements Partition.Delete
func (partition *partition) Delete() error {
	if err := partition.store.kvStore.Partition(partition.name).Delete(); err != nil {
		return fmt.Errorf("could not delete partition: %s", err.Error())
	}

	return nil
}

// Metadata implements Partition.Metadata
func (partition *partition) Metadata() ([]byte, error) {
	transaction, err := partition.beginTxn(false)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer transaction.Rollback()

	transaction = partition.metadataNamespace(transaction)

	return transaction.Get(metadataKey)
}

// Transaction implements Partition.Transaction
func (partition *partition) Transaction() (Transaction, error) {
	txn, err := partition.beginTxn(true)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	return &transaction{partition: partition, txn: txn}, nil
}

// View implements Partition.View
func (partition *partition) View(revision int64) (View, error) {
	txn, err := partition.beginTxn(false)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	newestRevision, err := partition.newestRevision(txn)

	if err != nil {
		txn.Rollback()

		return nil, fmt.Errorf("unable to retrieve current revision: %s", err.Error())
	}

	oldestRevision, err := partition.oldestRevision(txn)

	if err != nil {
		txn.Rollback()

		return nil, fmt.Errorf("unable to retrieve oldest revision: %s", err.Error())
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
		return nil, fmt.Errorf("could not calculate next revision number: %s", err.Error())
	}

	revisionsTxn := transaction.partition.revisionsNamespace(transaction.txn)

	if err := revisionsTxn.Put(newRevisionsKey(nextRevision, nil), []byte{}); err != nil {
		return nil, fmt.Errorf("could not insert revision start market: %s", err.Error())
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

	revisionsIter, err := newRevisionsIterator(transaction.partition.revisionsNamespace(transaction.txn), nil, &revisionsCursor{revision: revision - 1}, SortOrderAsc)

	if err != nil {
		return fmt.Errorf("could not create revisions iterator: %s", err.Error())
	}

	keysIter, err := newKeysIterator(transaction.partition.keysNamespace(transaction.txn), nil, nil, SortOrderAsc)

	if err != nil {
		return fmt.Errorf("could not create keys iterator: %s", err.Error())
	}

	for revisionsIter.next() {
	}

	for keysIter.next() {
	}

	return nil
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
	return nil
}

// Delete implements Revision.Delete
func (revision *revision) Delete(key []byte) error {
	return nil
}

type view struct {
	revision  int64
	partition *partition
	txn       kv.Transaction
}

// Keys implements View.Keys
func (view *view) Keys(min []byte, max []byte, limit int, order SortOrder) ([]KV, error) {
	iter, err := newViewRevisionsIterator(view.txn, min, max, view.revision, order)

	if err != nil {
		return nil, fmt.Errorf("could not create iterator: %s", err.Error())
	}

	var kvs []KV

	if limit >= 0 {
		kvs = make([]KV, 0, limit)
	} else {
		kvs = make([]KV, 0, 0)
	}

	for iter.next() && (limit < 0 || len(kvs) < limit) {
		kvs = append(kvs, KV{iter.key(), iter.value()})
	}

	if iter.error() != nil {
		return nil, fmt.Errorf("iteration error: %s", iter.error().Error())
	}

	return kvs, nil
}

// Changes implements View.Changes
func (view *view) Changes(min []byte, max []byte, limit int) ([]KV, error) {
	iter, err := newRevisionsIterator(view.partition.revisionsNamespace(view.txn), &revisionsCursor{revision: view.revision}, &revisionsCursor{revision: view.revision, key: max}, SortOrderAsc)

	if err != nil {
		return nil, fmt.Errorf("could not create revisions iterator: %s", err.Error())
	}

	// Read empty revision marker first to get to the actual data
	if !iter.next() {
		if iter.error() != nil {
			return nil, fmt.Errorf("iteration error: %s", err.Error())
		}

		// There is no revision marker for this revision so it must have been
		// compacted
		return nil, fmt.Errorf("consistency violation: revision %d should exist, but a record of it was not found", view.revision)
	}

	if iter.key() != nil {
		return nil, fmt.Errorf("consistency violation: the first key of each revision should be nil")
	}

	var kvs []KV

	if limit >= 0 {
		kvs = make([]KV, 0, limit)
	} else {
		kvs = make([]KV, 0, 0)
	}

	// Skip to the start key
	done := false
	for ; !done && bytes.Compare(iter.key(), min) < 0; done = !iter.next() {
	}

	if iter.error() != nil {
		return nil, fmt.Errorf("iteration error: %s", err.Error())
	} else if done {
		return kvs, nil
	}

	kvs = append(kvs, KV{iter.key(), iter.value()})

	for iter.next() && (limit < 0 || len(kvs) < limit) {
		kvs = append(kvs, KV{iter.key(), iter.value()})
	}

	if iter.error() != nil {
		return nil, fmt.Errorf("iteration error: %s", err.Error())
	}

	return kvs, nil
}

// Revision implements View.Revision
func (view *view) Revision() int64 {
	return view.revision
}

// Close implements View.Close
func (view *view) Close() error {
	return view.txn.Rollback()
}

type mvccIterator struct {
	iter       kv.Iterator
	parseKey   func(k []byte) ([]byte, int64, error)
	parseValue func(v []byte) ([]byte, error)
	k          []byte
	rev        int64
	v          []byte
	err        error
	done       bool
}

// advance and validate for format but without the internal consistency checks
func (iter *mvccIterator) n() (bool, []byte, []byte, int64, error) {
	if !iter.iter.Next() {
		return false, nil, nil, 0, iter.iter.Error()
	}

	k, revision, err := iter.parseKey(iter.iter.Key())

	if err != nil {
		return false, nil, nil, 0, fmt.Errorf("could not parse key %#v: %s", iter.iter.Key(), err)
	}

	if revision <= 0 {
		return false, nil, nil, 0, fmt.Errorf("consistency violation: revision number must be positive")
	}

	v, err := iter.parseValue(iter.iter.Value())

	if err != nil {
		return false, nil, nil, 0, fmt.Errorf("could not parse value %#v: %s", iter.iter.Value(), err)
	}

	return true, k, v, revision, nil
}

func (iter *mvccIterator) next() bool {
	if iter.done {
		return false
	}

	cont, key, value, revision, err := iter.n()

	if !cont {
		iter.done = true
		iter.err = err
		iter.k = nil
		iter.v = nil
		iter.rev = 0

		return false
	}

	iter.k = key
	iter.v = value
	iter.rev = revision

	return true
}

func (iter *mvccIterator) key() []byte {
	return iter.k
}

func (iter *mvccIterator) value() []byte {
	return iter.v
}

func (iter *mvccIterator) revision() int64 {
	return iter.rev
}

func (iter *mvccIterator) error() error {
	return nil
}

type revisionsCursor struct {
	revision int64
	key      []byte
}

func (c *revisionsCursor) bytes() revisionsKey {
	if c == nil {
		return nil
	}

	return newRevisionsKey(c.revision, c.key)
}

type revisionsIterator struct {
	mvccIterator
	order SortOrder
}

func newRevisionsIterator(txn kv.Transaction, min *revisionsCursor, max *revisionsCursor, order SortOrder) (*revisionsIterator, error) {
	iter, err := txn.Keys(min.bytes(), max.bytes(), kv.SortOrder(order))

	if err != nil {
		return nil, fmt.Errorf("could not create iterator: %s", err.Error())
	}

	return &revisionsIterator{
		mvccIterator: mvccIterator{
			iter: iter,
			parseKey: func(key []byte) ([]byte, int64, error) {
				k, err := revisionsKey(key).key()

				if err != nil {
					return nil, 0, fmt.Errorf("could not split key and revision in %#v: %s", key, err)
				}

				rev, err := revisionsKey(key).revision()

				if err != nil {
					return nil, 0, fmt.Errorf("could not split key and revision in %#v: %s", key, err)
				}

				return k, rev, nil
			},
			parseValue: func(value []byte) ([]byte, error) {
				return revisionsValue(value).value(), nil
			},
		}, order: order,
	}, nil
}

func (iter *revisionsIterator) next() bool {
	oldRev := iter.rev

	if !iter.mvccIterator.next() {
		return false
	}

	defer func() {
		if iter.err != nil {
			iter.done = true
		}

		if iter.done {
			iter.k = nil
			iter.v = nil
			iter.rev = 0
		}
	}()

	if oldRev != 0 && oldRev != iter.rev {
		if iter.order == SortOrderDesc && oldRev != iter.rev+1 {
			iter.err = fmt.Errorf("consistency violation: each revision must be exactly one less than the last: revision %d follows revision %d", iter.rev, oldRev)
		} else if oldRev != iter.rev-1 {
			iter.err = fmt.Errorf("consistency violation: each revision must be exactly one more than the last: revision %d follows revision %d", iter.rev, oldRev)
		}

		return false
	}

	return true
}

type keysCursor struct {
	revision int64
	key      []byte
}

func (c *keysCursor) bytes() keysKey {
	if c == nil {
		return nil
	}

	return newKeysKey(c.key, c.revision)
}

type keysIterator struct {
	mvccIterator
}

func newKeysIterator(txn kv.Transaction, min *keysCursor, max *keysCursor, order SortOrder) (*keysIterator, error) {
	iter, err := txn.Keys(min.bytes(), max.bytes(), kv.SortOrder(order))

	if err != nil {
		return nil, fmt.Errorf("could not create iterator: %s", err.Error())
	}

	return &keysIterator{
		mvccIterator{
			iter: iter,
			parseKey: func(key []byte) ([]byte, int64, error) {
				k, err := keysKey(key).key()

				if err != nil {
					return nil, 0, fmt.Errorf("could not split key and revision in %#v: %s", key, err)
				}

				rev, err := keysKey(key).revision()

				if err != nil {
					return nil, 0, fmt.Errorf("could not split key and revision in %#v: %s", key, err)
				}

				return k, rev, nil
			},
			parseValue: func(value []byte) ([]byte, error) {
				return value, nil
			},
		},
	}, nil
}

// Like keysIterator but it skips anything
type viewRevisionKeysIterator struct {
	keysIterator
	viewRevision int64
	currentKey   []byte
	currentValue []byte
	currentRev   int64
}

func newViewRevisionsIterator(txn kv.Transaction, min []byte, max []byte, revision int64, order SortOrder) (*viewRevisionKeysIterator, error) {
	keysIterator, err := newKeysIterator(txn, &keysCursor{key: min}, &keysCursor{key: max}, order)

	if err != nil {
		return nil, err
	}

	// advance to the first position (if any)
	keysIterator.next()

	return &viewRevisionKeysIterator{keysIterator: *keysIterator, viewRevision: revision}, nil
}

// return the highest revision of each key whose revision <= iter.viewRevision
func (iter *viewRevisionKeysIterator) next() bool {
	if iter.k == nil {
		return false
	}

	iter.currentKey = iter.k
	iter.currentRev = iter.rev
	iter.currentValue = iter.v
	done := false

	for ; !done && bytes.Compare(iter.currentKey, iter.k) == 0; done = !iter.keysIterator.next() {
		if iter.rev > iter.currentRev && iter.rev <= iter.viewRevision {
			iter.currentRev = iter.rev
			iter.currentValue = iter.v
		}
	}

	return !done
}

func (iter *viewRevisionKeysIterator) key() []byte {
	return iter.currentKey
}

func (iter *viewRevisionKeysIterator) value() []byte {
	return iter.currentValue
}

func (iter *viewRevisionKeysIterator) revision() int64 {
	return iter.currentRev
}
