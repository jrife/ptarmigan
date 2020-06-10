package mvcc

import (
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/storage/snapshot"
)

// Store is the interface for a partitioned MVCC
// store
type Store interface {
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
	// Delete closes then deletes this store and all its contents.
	Delete() error
	// Partitions returns up to limit partition names in ascending
	// lexocographical order from the names range. limit < 0
	// means no limit. It must return ErrClosed if its invocation starts
	// after Close() returns.
	Partitions(names keys.Range, limit int) ([][]byte, error)
	// Partition returns a handle to the partition with this name.
	// It does not guarantee that this partition exists yet and
	// should not create the partition. It must not return nil.
	Partition(name []byte) Partition
}

// Partition represents a partition of a store
type Partition interface {
	// Name returns the name of this partition
	Name() []byte
	// Create creates this partition if it does not exist. It has no
	// effect if the partition already exists. It must return ErrClosed
	// if its invocation starts after Close() on the store returns.
	// metadata is set only if this call actually creates the partition.
	Create(metadata []byte) error
	// Delete deletes this partition if it exists. It has no effect if
	// the partition does not exist. It must return ErrClosed if its
	// invocation starts after Close() on the store returns.
	Delete() error
	// Metadata returns the metadata that was passed in during
	// partition creation. It must return ErrClosed if its
	// invocation starts after Close() on the store returns. Otherwise
	// if this partition does not exist it must return ErrNoSuchPartition.
	Metadata() ([]byte, error)
	// Begin starts a read-write transaction. Only
	// one read-write transaction can run at one time per
	// partition. This function must block if another transaction
	// exists that has not yet committed or rolled back.
	// It must return ErrClosed if its invocation starts after
	// Close() on the store returns. Otherwise if this partition
	// does not exist it must return ErrNoSuchPartition.
	Begin(writeable bool) (Transaction, error)
	// Snapshot takes a consistent snapshot of this partition. If Snapshot() is called
	// after Close() on the store returns it must return ErrClosed. Otherwise if this
	// partition does not exist it must return ErrNoSuchPartition.
	snapshot.Source
	// ApplySnapshot applies a snapshot to this partition. If ApplySnapshot() is called
	// after Close() on the store returns it must return ErrClosed. If this partition
	// doesn't exist ApplySnapshot will create it. If the partition does exist ApplySnapshot
	// overwrites the state currently stored in the partition.
	snapshot.Acceptor
}

// Transaction lets a user manipulate the state of the partition.
type Transaction interface {
	// NewRevision creates a new revision. Each transaction
	// can create at most one new revision. Calling this
	// more than once on a transaction must return ErrTooManyRevisions.
	// The revision number for the returned revision must
	// be exactly one more than the newest revision, or if this
	// store is brand new the first revision applied to it
	// must have a revision number of 1. Revision numbers must
	// be contiguous. If this is a read-only transaction it must
	// return ErrReadOnly
	NewRevision() (Revision, error)
	// View returns a view that lets a user
	// inspect the state of the store at some revision.
	// If revision == 0 it selects the newest revision.
	// If revision < 0 it selects the oldest revision.
	// If revision is
	// higher than the newest committed revision this
	// must return ErrRevisionTooHigh. If revision is
	// lower than the oldest revision it must return
	// ErrCompacted.
	View(revision int64) (View, error)
	// Compact deletes all revision history up to the
	// specified revision (exclusive).
	// If revision == 0 it selects the newest revision.
	// If revision < 0 it selects the oldest revision.
	// If revision is
	// higher than the newest committed revision this
	// must return ErrRevisionTooHigh. If revision is
	// lower than the oldest revision it must return
	// ErrCompacted. If this is a read-only transaction it must
	// return ErrReadOnly
	Compact(revision int64) error
	// Flat returns a handle to a flat kv.Map associated
	// with this mvcc partition. This provides a way to
	// read and write non-versioned keys within a transaction.
	// Keys in the flat map do not overlap with versioned keys.
	Flat() kv.Map
	// Commit commits the changes made in this transaction.
	// If this is a read-only transaction it must
	// return ErrReadOnly
	Commit() error
	// Rolls back the transaction.
	Rollback() error
}

// Revision is created as part of a transaction. It lets
// a user update the keys within a partition.
type Revision interface {
	View
	kv.MapUpdater
}

// View lets a user read the state of the store
// at a certain revision.
type View interface {
	kv.MapReader
	// Next returns a view for the revision after
	// this one.
	//
	// Returns ErrRevisionTooHigh if the current view is for the newest revision.
	Next() (View, error)
	// Next returns a view for the revision before
	// this one.
	//
	// Returns ErrCompacted if the current view is for the oldest revision.
	Prev() (View, error)
	// Changes returns up to limit keys changed in this revision
	// lexocographically increasing order from the specified range.
	// If includePrev is true the returned diffs will include the
	// previous state for each key. Otherwise only the state as of
	// this revision will be set.
	Changes(keys keys.Range) (DiffIterator, error)
	// Return the revision for this view.
	Revision() int64
}

// DiffIterator lets a consumer iterate through changes made
// at some revision.
type DiffIterator interface {
	kv.Iterator
	IsPut() bool
	IsDelete() bool
}
