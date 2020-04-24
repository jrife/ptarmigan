package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/jrife/ptarmigan/storage/kv"
)

var (
	metadataBucket  = []byte{0}
	keysBucket      = []byte{1}
	revisionsBucket = []byte{2}
	metadataKey     = []byte{0}
)

// b must be a byte slice of length 8
func bytesToInt64(b []byte) int64 {
	if len(b) != 8 {
		panic("b must be length 8")
	}

	n, _ := binary.Varint(b)

	return n
}

// b must be a byte slice of length 8
func int64ToBytes(b []byte, n int64) {
	if len(b) != 8 {
		panic("b must be length 8")
	}

	binary.PutVarint(b, n)
}

func min(a int, b int) int {
	if a > b {
		return b
	}

	return a
}

// New creates a new mvcc store
func New(kvStore kv.Store) Store {
	store := &store{
		kvStore: kvStore,
	}

	store.revisions.revisions = map[string]int64{}

	return store
}

var _ Store = (*store)(nil)

type store struct {
	kvStore   kv.SubStore
	revisions struct {
		sync.Mutex
		revisions map[string]int64
	}
}

// Open and initialize store
func (store *store) Open() error {
	transaction, err := store.kvStore.Begin(true)

	if err != nil {
		return fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer transaction.Rollback()

	// Always ensure that the root bucket exists
	if err := transaction.Root().Create(); err != nil {
		return fmt.Errorf("could not ensure root bucket exists: %s", err.Error())
	}

	// Figure out the last revision for each partition
	partitionNames, err := store.Partitions(nil, nil, -1)

	if err != nil {
		return fmt.Errorf("could not list partition names: %s", err.Error())
	}

	for _, name := range partitionNames {
		partitionBucket := transaction.Root().Bucket(name)
		lastRevisionKey, _ := partitionBucket.Bucket(revisionsBucket).Cursor().Last()
		var lastRevision int64 = 0

		// nil key indicates an empty partition that has not yet had any revisions
		// added to it
		if lastRevisionKey != nil {

			if len(lastRevisionKey) < 8 {
				return fmt.Errorf("revision key in partition %s was too short. expected it to be >= 8 bytes long: %#v", string(name), lastRevisionKey)
			}

			lastRevision = bytesToInt64(lastRevisionKey)
		}

		store.revisions.revisions[string(name)] = lastRevision
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %s", err.Error())
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
func (store *store) Partitions(start []byte, end []byte, limit int) ([][]byte, error) {
	transaction, err := store.kvStore.Begin(false)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer transaction.Rollback()

	var partitions [][]byte

	if limit < -1 {
		partitions = make([][]byte, 0, len(store.revisions.revisions))
	} else {
		partitions = make([][]byte, 0, limit)
	}

	cursor := transaction.Root().Cursor()

	for k, _ := cursor.Seek(start); k != nil && len(partitions) < limit && bytes.Compare(k, end) < 0; k, _ = cursor.Next() {
		partitions = append(partitions, k)
	}

	return partitions, nil
}

// Partition returns a handle to the named partition
func (store *store) Partition(name []byte) Partition {
	return &partition{store: store, name: name}
}

type partition struct {
	store *store
	name  []byte
}

func (partition *partition) beginTxn(write bool) (kv.Transaction, error) {
	return partition.store.kvStore.Namespace(partition.name).Begin(write)
}

// Return partition name
func (partition *partition) Name() []byte {
	return partition.name
}

// Create creates this partition if it does not exist and
// assigns it some metadata that can be retrieved with
// Metadata()
func (partition *partition) Create(metadata []byte) error {
	transaction, err := partition.beginTxn(true)

	if err != nil {
		return fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer transaction.Rollback()

	// do nothing if this partition already exists
	if transaction.Root().Exists() {
		return nil
	}

	// initialize empty state
	if err := transaction.Root().Create(); err != nil {
		return fmt.Errorf("could not create partition bucket: %s", err.Error())
	}

	for _, bucket := range [][]byte{metadataBucket, keysBucket, revisionsBucket} {
		if err := transaction.Root().Bucket(bucket).Create(); err != nil {
			return fmt.Errorf("could not create bucket inside of partition bucket %#v: %s", bucket, err.Error())
		}
	}

	if err := transaction.Root().Bucket(metadataBucket).Put(metadataKey, metadata); err != nil {
		return fmt.Errorf("could not set metadata for partition: %s", err.Error())
	}

	// release the lock only after commit for every read-write
	// transaction so that the next transaction observes a map
	// state that is consistent with the kv store state. It is
	// possible that another transaction is waiting to start but
	// is currently blocked by this transaction. Once commit
	// returns the next transaction may start and try to read
	// the revisions map. That map state must be consistent with
	// the current disk state.
	partition.store.revisions.Lock()
	defer partition.store.revisions.Unlock()
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %s", err.Error())
	}

	partition.store.revisions.revisions[string(partition.name)] = 0

	return nil
}

// Delete deletes this partition and all its data if it
// exists.
func (partition *partition) Delete() error {
	transaction, err := partition.beginTxn(true)

	if err != nil {
		return fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer transaction.Rollback()

	// do nothing if this partition doesn't exists
	if !transaction.Root().Exists() {
		return nil
	}

	if err := transaction.Root().Purge(); err != nil {
		return fmt.Errorf("could not delete partition bucket: %s", err.Error())
	}

	// release the lock only after commit for every read-write
	// transaction so that the next transaction observes a map
	// state that is consistent with the kv store state. It is
	// possible that another transaction is waiting to start but
	// is currently blocked by this transaction. Once commit
	// returns the next transaction may start and try to read
	// the revisions map. That map state must be consistent with
	// the current disk state.
	partition.store.revisions.Lock()
	defer partition.store.revisions.Unlock()
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %s", err.Error())
	}

	delete(partition.store.revisions.revisions, string(partition.name))

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

	// do nothing if this partition already exists
	if !transaction.Root().Exists() {
		return nil, ErrNoSuchPartition
	}

	if !transaction.Root().Bucket(metadataBucket).Exists() {
		return nil, fmt.Errorf("could not find metadata bucket")
	}

	metadata, err := transaction.Root().Bucket(metadataBucket).Get(metadataKey)

	if err != nil {
		return nil, fmt.Errorf("could not get metadata key: %s", err.Error())
	}

	return metadata, nil
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

	if !txn.Root().Exists() {
		txn.Rollback()

		return nil, ErrNoSuchPartition
	}

	partition.store.revisions.Lock()
	lastRevision, ok := partition.store.revisions.revisions[string(partition.name)]
	partition.store.revisions.Unlock()

	if !ok {
		txn.Rollback()

		return nil, ErrNoSuchPartition
	}

	return &transaction{partition: partition, txn: txn, revisionNumber: lastRevision + 1}, nil
}

// View generates a handle that lets a user
// inspect the state of the store at some
// revision
func (partition *partition) View(revision int64) View {
	return &view{partition: partition, revision: revision, kvStore: partition.store.kvStore.Namespace(partition.name)}
}

// ApplySnapshot completely replaces the contents of this
// store with those in this snapshot.
func (partition *partition) ApplySnapshot(snap io.Reader) error {
	return partition.store.kvStore.Namespace(partition.name).ApplySnapshot(snap)
}

// Snapshot takes a snapshot of this store's current state
// and encodes it as a byte stream. It must be compatible with
// ApplySnapshot() such that its return value could be applied
// to ApplySnapshot() in order to replicate its state elsewhere.
func (partition *partition) Snapshot() (io.Reader, error) {
	return partition.store.kvStore.Namespace(partition.name).Snapshot()
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
// The revision number for the returned revision must
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
	revision  int64
	partition *partition
	kvStore   kv.SubStore
	txn       kv.Transaction
}

func (view *view) transaction() (kv.Transaction, bool, error) {
	if view.txn != nil {
		return view.txn, false, nil
	}

	txn, err := view.kvStore.Begin(false)

	if err != nil {
		return nil, false, fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	return txn, true, nil
}

// Get returns up to limit keys at the revision seen by this view
// lexocographical order where keys are >= start and < end. start = nil
// means the lowest key. end = nil means the highest key. limit <= -1
// means no limit.
func (view *view) Keys(start []byte, end []byte, limit int) ([]KV, error) {
	txn, rollback, err := view.transaction()

	if err != nil {
		return nil, fmt.Errorf("could not obtain transaction for request: %s", err.Error())
	}

	if rollback {
		defer txn.Rollback()
	}

	b := make([]byte, 8)
	int64ToBytes(b, view.revision)
	revisionBucket := view.txn.Root().Bucket(revisionsBucket).Bucket(b)

	// determine which error to return. this should only be a concern
	// when the view is created directly from a partition as opposed to
	// when it is being called from within a revision
	if !revisionBucket.Exists() {
		view.partition.store.revisions.Lock()
		latestRevision, ok := view.partition.store.revisions.revisions[string(view.partition.name)]
		view.partition.store.revisions.Unlock()

		if !ok {
			return nil, ErrNoSuchPartition
		}

		if view.revision < latestRevision {
			return nil, ErrCompacted
		}

		if view.revision > latestRevision {
			return nil, ErrRevisionTooHigh
		}
	}

	return nil, nil
}

// Changes returns up to limit keys changed in this revision
// lexocographical order where keys are >= start and < end.
// start = nil means the lowest key end = nil means the highest key.
// limit <= -1 means no limit.
func (view *view) Changes(start []byte, end []byte, limit int) ([]KV, error) {
	return nil, nil
}

// Return the revision for this view.
func (view *view) Revision() int64 {
	return view.revision
}
