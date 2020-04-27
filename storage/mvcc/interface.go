package mvcc

import (
	"errors"
	"io"

	"github.com/jrife/ptarmigan/storage/kv"
)

// SortOrder describes sort order for keys
// Either SortOrderAsc or SortOrderDesc
type SortOrder kv.SortOrder

const (
	// SortOrderAsc sorts in increasing order
	SortOrderAsc = SortOrder(kv.SortOrderAsc)
	// SortOrderDesc sorts in decreasing order
	SortOrderDesc = SortOrder(kv.SortOrderDesc)
	// RevisionOldest can be used in place of a revision
	// number to access the oldest revision
	RevisionOldest int64 = -1
	// RevisionNewest can be used in place of a revision
	// number to access the neweset revision
	RevisionNewest int64 = 0
)

var (
	// ErrCompacted is returned when an operation cannot be completed
	// because it needs to read data that was compacted away.
	ErrCompacted = errors.New("store was compacted")
	// ErrRevisionTooHigh is returned when an operation tries to access
	// a revision number that is higher than the newest committed revision.
	ErrRevisionTooHigh = errors.New("revision number is higher than the newest committed revision")
	// ErrNoSuchPartition is returned when a function tries to access a partition
	// that does not exist.
	ErrNoSuchPartition = errors.New("partition does not exist")
	// ErrNoRevisions is returned when a consumer requests a view of either the oldest
	// or newest revision, but the partition is empty having had no revisions
	// written to it yet.
	ErrNoRevisions = errors.New("partition has no revision")
)

// KV is a key-value pair
type KV [2][]byte

// Store is the interface for a partitioned MVCC
// store
type Store interface {
	// Open and initialize store
	Open() error
	// Close store
	Close() error
	// Partitions returns up to limit partition names in
	// lexocographical order where names are >= min and < max. min = nil
	// means the lowest name. max = nil means the highest name. limit < 0
	// means no limit.
	Partitions(min []byte, max []byte, limit int) ([][]byte, error)
	// Partition returns a handle to the named partition
	Partition(name []byte) Partition
}

// Partition represents an independent
// partition of the store. Each partition
// can be completely independent
type Partition interface {
	// Return partition name
	Name() []byte
	// Create creates this partition if it does not exist and
	// assigns it some metadata that can be retrieved with
	// Metadata()
	Create(metadata []byte) error
	// Delete deletes this partition and all its data if it
	// exists.
	Delete() error
	// Metadata returns the metadata that was passed in during
	// partition creation.
	Metadata() ([]byte, error)
	// Transaction starts a read-write transaction. Only
	// one read-write transaction can run at one time.
	// This function must block if another transaction
	// exists that has not yet committed or rolled back.
	Transaction() (Transaction, error)
	// View generates a handle that lets a user
	// inspect the state of the store at some
	// revision. If revision == 0 it selects the
	// the newest revision. If revisino < 0 it selects
	// the oldest revision.
	View(revision int64) (View, error)
	// ApplySnapshot completely replaces the contents of this
	// store with those in this snapshot.
	ApplySnapshot(snap io.Reader) error
	// Snapshot takes a snapshot of this store's current state
	// and encodes it as a byte stream. It must be compatible with
	// ApplySnapshot() such that its return value could be applied
	// to ApplySnapshot() in order to replicate its state elsewhere.
	Snapshot() (io.Reader, error)
}

// Transaction lets a user manipulate the state
// of the store. Changes made inside a transaction
// must not be visible to observers until the
// transaction is committed.
type Transaction interface {
	// NewRevision creates a new revision. Each transaction
	// can create at most one new revision. Calling this
	// more than once on a transaction must return an error.
	// The revision number for the returned revision must
	// be exactly one more than the newest revision, or if this
	// store is brand new the first revision applied to it
	// must have a revision number of 1. Revision numbers must
	// be contiguous.
	NewRevision() (Revision, error)
	// Compact deletes all revision history up to the
	// specified revision (exclusive). If revision is
	// higher than the newest committed revision this
	// must return ErrRevisionTooHigh. If revision is
	// lower than the oldest revision it must return
	// ErrCompacted.
	Compact(revision int64) error
	// Commit commits the changes made in this transaction.
	Commit() error
	// Rolls back the transaction.
	Rollback() error
}

// Revision is created as part of a transaction. It lets
// a user change the state of the store.
type Revision interface {
	View
	// Put creates or updates a key
	Put(key []byte, value []byte) error
	// Delete deletes a key
	Delete(key []byte) error
}

// View lets a user read the state of the store
// at a certain revision.
type View interface {
	// Keys returns up to limit keys at the revision seen by this view
	// where keys are >= min and < max. min = nil means the lowest key.
	// max = nil means the highest key. limit < 0 means no limit. sort
	// = SortOrderAsc means return keys in lexicographically increasing order
	// sort = SortOrderDesc means return keys in lexicographically decreasing
	// order.
	Keys(min []byte, max []byte, limit int, sort SortOrder) ([]KV, error)
	// Changes returns up to limit keys changed in this revision
	// lexocographically increasing order where keys are >= min and < max.
	// min = nil means the lowest key max = nil means the highest key.
	// limit < 0 means no limit.
	Changes(min []byte, max []byte, limit int) ([]KV, error)
	// Return the revision for this view.
	Revision() int64
	// Close must be called when a user is done with a view.
	Close() error
}
