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

// revision must be >= 0
func combineKeyAndRevision(key []byte, revision int64) []byte {
	keyAndRevision := make([]byte, len(key)+9)
	int64ToBytes(keyAndRevision[len(key)+1:], revision)

	copy(keyAndRevision[:len(key)], key)
	keyAndRevision[len(key)+1] = 0

	return keyAndRevision
}

func splitKeyAndRevision(keyAndRevision []byte) ([]byte, int64, error) {
	if len(keyAndRevision) < 9 {
		return nil, 0, fmt.Errorf("keyAndRevision not long enough to be valid")
	}

	revision := bytesToInt64(keyAndRevision[len(keyAndRevision)-8:])

	return keyAndRevision[:len(keyAndRevision)-9], revision, nil
}

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

// Open and initialize store
func (store *store) Open() error {
	// Always ensure that the store exists
	if err := store.kvStore.Create(); err != nil {
		return fmt.Errorf("could not ensure store exists: %s", err.Error())
	}

	return nil
}

// Close store
func (store *store) Close() error {
	return nil
}

// Partitions returns up to limit partition names in
// lexocographical order where names are >= start and < end. start = nil
// means the lowest name. end = nil means the highest name. limit <= -1
// means no limit.
func (store *store) Partitions(min []byte, max []byte, limit int) ([][]byte, error) {
	return store.kvStore.Partitions(min, max, limit)
}

// Partition returns a handle to the named partition
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
	transaction = kv.Namespace(transaction, revisionsPrefix)
	iter, err := transaction.Keys(nil, nil, kv.SortOrderDesc)

	if err != nil {
		return 0, fmt.Errorf("could not create iterator: %s", err.Error())
	}

	var newestRevision int64 = 0

	if !iter.Next() {
		if iter.Error() != nil {
			return 0, fmt.Errorf("iteration error: %s", err.Error())
		}

		return 0, nil
	}

	newestRevisionBytes := iter.Key()

	if len(newestRevisionBytes) < 8 {
		return 0, fmt.Errorf("could not extract revision number from revision key: it is too short")
	}

	newestRevision = bytesToInt64(newestRevisionBytes[:8])

	if newestRevision < 0 {
		return 0, fmt.Errorf("newestRevision is negative, this shouldn't happen")
	}

	return newestRevision, nil
}

func (partition *partition) oldestRevision(transaction kv.Transaction) (int64, error) {
	transaction = kv.Namespace(transaction, revisionsPrefix)
	iter, err := transaction.Keys(nil, nil, kv.SortOrderAsc)

	if err != nil {
		return 0, fmt.Errorf("could not create iterator: %s", err.Error())
	}

	var oldestRevision int64 = 0

	if !iter.Next() {
		if iter.Error() != nil {
			return 0, fmt.Errorf("iteration error: %s", err.Error())
		}

		return 0, nil
	}

	oldestRevisionBytes := iter.Key()

	if len(oldestRevisionBytes) < 8 {
		return 0, fmt.Errorf("could not extract revision number from revision key: it is too short")
	}

	oldestRevision = bytesToInt64(oldestRevisionBytes[:8])

	if oldestRevision < 0 {
		return 0, fmt.Errorf("oldestRevision is negative, this shouldn't happen")
	}

	return oldestRevision, nil
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

// Return partition name
func (partition *partition) Name() []byte {
	return partition.name
}

// Create creates this partition if it does not exist and
// assigns it some metadata that can be retrieved with
// Metadata()
func (partition *partition) Create(metadata []byte) error {
	if err := partition.store.kvStore.Partition(partition.name).Create(); err != nil {
		return fmt.Errorf("could not create partition: %s", err.Error())
	}

	return nil
}

// Delete deletes this partition and all its data if it
// exists.
func (partition *partition) Delete() error {
	if err := partition.store.kvStore.Partition(partition.name).Delete(); err != nil {
		return fmt.Errorf("could not delete partition: %s", err.Error())
	}

	return nil
}

// Metadata returns the metadata that was passed in during
// partition creation.
func (partition *partition) Metadata() ([]byte, error) {
	transaction, err := partition.beginTxn(false)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer transaction.Rollback()

	transaction = kv.Namespace(transaction, metadataPrefix)

	return transaction.Get(metadataKey)
}

// Transaction starts a read-write transaction. Only
// one read-write transaction can run at one time.
// This function must block if another transaction
// exists that has not yet committed or rolled back.
func (partition *partition) Transaction() (Transaction, error) {
	txn, err := partition.beginTxn(true)

	if err != nil {
		txn.Rollback()

		return nil, fmt.Errorf("could not begin transaction: %s", err)
	}

	nextRevision, err := partition.nextRevision(txn)

	if err != nil {
		txn.Rollback()

		return nil, fmt.Errorf("could not calculate next revision number: %s", err.Error())
	}

	return &transaction{partition: partition, txn: txn, revisionNumber: nextRevision}, nil
}

// View generates a handle that lets a user
// inspect the state of the store at some
// revision
func (partition *partition) View(revision int64) View {
	return &view{partition: partition, revision: revision, kvPartition: partition.store.kvStore.Partition(partition.name)}
}

// ApplySnapshot completely replaces the contents of this
// store with those in this snapshot.
func (partition *partition) ApplySnapshot(snap io.Reader) error {
	return partition.store.kvStore.Partition(partition.name).ApplySnapshot(snap)
}

// Snapshot takes a snapshot of this store's current state
// and encodes it as a byte stream. It must be compatible with
// ApplySnapshot() such that its return value could be applied
// to ApplySnapshot() in order to replicate its state elsewhere.
func (partition *partition) Snapshot() (io.Reader, error) {
	return partition.store.kvStore.Partition(partition.name).Snapshot()
}

type transaction struct {
	partition      *partition
	txn            kv.Transaction
	revision       Revision
	revisionNumber int64
}

// NewRevision creates a new revision. Each transaction
// can create at most one new revision. Calling this
// more than once on a transaction must return an error.
// The revision number for the returned revision mustkvStore
// be exactly one more than the newest revision, or if this
// store is brand new the first revision applied to it
// must have a revision number of 1. Revision numbers must
// be contiguous.
func (transaction *transaction) NewRevision() (Revision, error) {
	if !transaction.txn.Root().Exists() {
		return nil, ErrNoSuchPartition
	}

	b := make([]byte, 8)
	int64ToBytes(b, transaction.revisionNumber)

	if err := transaction.txn.Root().Bucket(revisionsBucket).Bucket(b).Create(); err != nil {
		return nil, fmt.Errorf("could not create revision bucket for revision %d: %s", transaction.revisionNumber, err.Error())
	}

	return &revision{view: view{partition: transaction.partition, txn: transaction.txn, revision: transaction.revisionNumber}}, nil
}

// Compact deletes all revision history up to the
// specified revision (exclusive). If revision is
// higher than the newest committed revision this
// must return ErrRevisionTooHigh. If revision is
// lower than the oldest revision it must return
// ErrCompacted.
func (transaction *transaction) Compact(revision int64) error {
	return nil
}

// Commit commits the changes made in this transaction.
func (transaction *transaction) Commit() error {
	transaction.partition.store.revisions.Lock()
	defer transaction.partition.store.revisions.Unlock()

	if err := transaction.txn.Commit(); err != nil {
		return err
	}

	if transaction.revision != nil {
		transaction.partition.store.revisions.revisions[string(transaction.partition.name)] = transaction.revisionNumber
	}

	return nil
}

// Rolls back the transaction.
func (transaction *transaction) Rollback() error {
	return transaction.txn.Rollback()
}

type revision struct {
	view
}

// Put creates or updates a key
func (revision *revision) Put(key []byte, value []byte) error {
	return nil
}

// Delete deletes a key
func (revision *revision) Delete(key []byte) error {
	return nil
}

type view struct {
	revision    int64
	partition   *partition
	kvPartition kv.Partition
	txn         kv.Transaction
}

func (view *view) transaction() (kv.Transaction, bool, error) {
	if view.txn != nil {
		return view.txn, false, nil
	}

	txn, err := view.kvPartition.Begin(false)

	if err != nil {
		return nil, false, fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	return txn, true, nil
}

// Get returns up to limit keys at the revision seen by this view
// lexocographical order where keys are >= start and < end. start = nil
// means the lowest key. end = nil means the highest key. limit <= -1
// means no limit.
func (view *view) Keys(min []byte, max []byte, limit int) ([]KV, error) {
	txn, rollback, err := view.transaction()

	if err != nil {
		return nil, fmt.Errorf("could not obtain transaction for request: %s", err.Error())
	}

	if rollback {
		defer txn.Rollback()
	}

	b := make([]byte, 8)
	int64ToBytes(b, view.revision)
	txn = kv.Namespace(txn, keysPrefix)
	iter, err := txn.Keys(min, max, kv.SortOrderAsc)

	if err != nil {
		return nil, fmt.Errorf("could not create iterator: %s", err.Error())
	}

	newestRevision, err := view.partition.newestRevision(txn)

	if err != nil {
		return nil, fmt.Errorf("unable to retrieve current revision: %s", err.Error())
	}

	oldestRevision, err := view.partition.oldestRevision(txn)

	if err != nil {
		return nil, fmt.Errorf("unable to retrieve oldest revision: %s", err.Error())
	}

	if view.revision > newestRevision {
		return nil, ErrRevisionTooHigh
	}

	if view.revision < oldestRevision {
		return nil, ErrCompacted
	}

	var kvs []KV

	if limit >= 0 {
		kvs = make([]KV, 0, limit)
	} else {
		kvs = make([]KV, 0, 0)
	}

	var prevKey []byte
	var prevValue []byte
	var prevRev int64

	for iter.Next() && (limit < 0 || len(kvs) < limit) {
		key, revision, err := splitKeyAndRevision(iter.Key())

		if err != nil {
			return nil, fmt.Errorf("key format was not valid: %#v: %s", iter.Key(), err)
		}

		if revision <= 0 {
			return nil, fmt.Errorf("invalid revision %d detected for key %#v: %s", revision, key, err)
		}

		// The first iteration
		if prevKey == nil && prevValue == nil && prevRev == 0 {
			prevKey = key
			prevValue = iter.Value()
			prevRev = revision

			continue
		}

		switch bytes.Compare(prevKey, key) {
		case 0:
			if revision < prevRev {
				return nil, fmt.Errorf("consistency violation detected: both previous and current key are %#v, but previous revision %#v > current revision %#v", key, prevRev, revision)
			}

			if revision > view.revision {
			}

			prevValue = iter.Value()
			prevRev = revision
		case -1:
		case 1:
			return nil, fmt.Errorf("consistency violation detected: previous key #%v is > current key #%v", prevKey, key)
		}

		kvs = append(kvs, KV{
			prevKey,
			prevValue,
		})
	}
	// For each

	return kvs, nil
}

// Changes returns up to limit keys changed in this revision
// lexocographical order where keys are >= start and < end.
// start = nil means the lowest key end = nil means the highest key.
// limit <= -1 means no limit.
func (view *view) Changes(min []byte, max []byte, limit int) ([]KV, error) {
	txn, rollback, err := view.transaction()

	if err != nil {
		return nil, fmt.Errorf("could not obtain transaction for request: %s", err.Error())
	}

	if rollback {
		defer txn.Rollback()
	}

	b := make([]byte, 8)
	int64ToBytes(b, view.revision)
	txn = kv.Namespace(kv.Namespace(txn, revisionsPrefix), b)
	iter, err := txn.Keys(min, max, kv.SortOrderAsc)

	if err != nil {
		return nil, fmt.Errorf("could not create iterator: %s", err.Error())
	}

	newestRevision, err := view.partition.newestRevision(txn)

	if err != nil {
		return nil, fmt.Errorf("unable to retrieve current revision: %s", err.Error())
	}

	if view.revision > newestRevision {
		return nil, ErrRevisionTooHigh
	}

	// Read empty revision marker first to get to the actual data
	if !iter.Next() {
		if iter.Error() != nil {
			return nil, fmt.Errorf("iteration error: %s", err.Error())
		}

		return nil, ErrCompacted
	}

	if len(iter.Key()) != 0 {
		return nil, fmt.Errorf("consistency problem: the first key of each revision should be empty")
	}

	var kvs []KV

	if limit >= 0 {
		kvs = make([]KV, 0, limit)
	} else {
		kvs = make([]KV, 0, 0)
	}

	for iter.Next() && (limit < 0 || len(kvs) < limit) {
		kvs = append(kvs, KV{iter.Key(), iter.Value()})
	}

	if iter.Error() != nil {
		return nil, fmt.Errorf("iteration error: %s", err.Error())
	}

	return kvs, nil
}

// Return the revision for this view.
func (view *view) Revision() int64 {
	return view.revision
}
