package storage

import (
	"context"
	"errors"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/snapshot"
)

var (
	// ErrNoSuchReplicaStore is returned when a replica store
	// is requested that does not exist.
	ErrNoSuchReplicaStore = errors.New("no such replica store")
	// ErrNoSuchLease is returned when an operation tries to access a key
	// that does not exist at the current revision.
	ErrNoSuchLease = errors.New("no such lease")
	// ErrIndexNotMonotonic is returned if an update index is used that is
	// not greater than the index for the previous update.
	ErrIndexNotMonotonic = errors.New("update index is less than or equal to the last update index")
	// ErrClosed is returned when the store is closed
	ErrClosed = errors.New("store is closed")
)

var (
	leasesNs       = []byte{0}
	updateIndexNs  = []byte{1}
	kvsNs          = []byte{2}
	updateIndexKey = []byte{0}
)

// Store describes a storage interface
// for a ptarmigan node.
type Store interface {
	// ReplicaStores lets a consumer iterate through
	// all the replica stores for this node. Replica
	// stores are returned in order by their name.
	// Returns a list of ReplicaStores whose name is
	// > start up to the specified limit.
	//
	// Returns ErrClosed if it is called after the store is closed
	ReplicaStores(ctx context.Context, start string, limit int) ([]string, error)
	// ReplicaStore returns a handle to the replica
	// store with the given name.
	ReplicaStore(name string) ReplicaStore
	// Close closes the store. Function calls to any I/O objects
	// descended from this store occurring after Close returns
	// must have no effect and return ErrClosed. Close must not
	// return until all concurrent I/O operations have concluded.
	// Operations started after the call to Close is started but
	// before it returns may proceed normally or may return ErrClosed.
	// If they return ErrClosed they must have no effect. Close may
	// return an error to indicate any problems that occurred during
	// shutdown.
	Close() error
	// Purge deletes all persistent data associated with this store.
	// This must only be called on a closed store. Returns ErrClosed
	// if it is called after the store is closed
	Purge() error
}

// ReplicaStore describes a storage interface for a ptarmigan replica.
type ReplicaStore interface {
	// Name returns the name of this replica store
	Name() string
	// Create initializes this replica store with some metadata.
	// It does nothing if the replica store already exists.
	//
	// Returns ErrClosed if it is called after the store is closed
	Create(ctx context.Context, metadata []byte) error
	// Delete deletes this replica store. It does nothing if the
	// replica store doesn't exist.
	//
	// Returns ErrClosed if it is called after the store is closed
	Delete(ctx context.Context) error
	// Metadata retrieves the metadata associated with this replica store.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	Metadata(ctx context.Context) ([]byte, error)
	// Index returns the latest index applied to this store.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	Index(ctx context.Context) (uint64, error)
	// Apply an update with this update index. Update indices must be
	// monotonically increasing. In other words, subsequent updates
	// must have increasing index numbers.
	Apply(index uint64) Update
	// Query executes a query.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	// Returns mvcc.ErrRevisionTooHigh if the revision specified in the query doesn't exist
	// Returns mvcc.ErrCompacted if the revision specified in the query has been compacted
	Query(ctx context.Context, query ptarmiganpb.KVQueryRequest) (ptarmiganpb.KVQueryResponse, error)
	// Changes returns a set of change events associated with this
	// revision. Events are always sorted in ascending order by the
	// byte order of their key. Returns events whose key is > start up
	// to limit. In other words, start acts as a cursor and limit
	// limits the size of the resulting events.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	// Returns mvcc.ErrRevisionTooHigh if the revision specified in the query doesn't exist
	// Returns mvcc.ErrCompacted if the revision specified in the query has been compacted
	Changes(ctx context.Context, watch ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error)
	// Leases lists all leases stored in this replica store.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	Leases(ctx context.Context) ([]ptarmiganpb.Lease, error)
	// GetLease reads the lease with this ID out of the replica store.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	// Returns ErrNoSuchLease if a lease with this ID does not exist
	GetLease(ctx context.Context, id int64) (ptarmiganpb.Lease, error)
	// NewestRevision returns the newest revision number
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	// Returns mvcc.ErrNoRevisions if there are no revisions
	NewestRevision() (int64, error)
	// OldestRevision returns the oldest revision number
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	// Returns mvcc.ErrNoRevisions if there are no revisions
	OldestRevision() (int64, error)
	// ApplySnapshot completely replaces the contents of this replica
	// store with those in this snapshot. If the replica store does not
	// exist it will be created.
	//
	// Returns ErrClosed if it is called after the store is closed
	snapshot.Acceptor
	// Snapshot takes a snapshot of this replica store's current state
	// and encode it as a byte stream. It must be compatible with
	// ApplySnapshot() such that its return value could be applied
	// to ApplySnapshot() in order to replicate its state elsewhere.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	snapshot.Source
}

// Update describes an update to a replica store
type Update interface {
	// Txn executes a transaction. A transaction creates a new revision
	// only if its execution results in running a put or delete operation.
	// A transaction whose execution results only in read operations will
	// not create a new revision.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	// Returns ErrNoSuchLease if a put in the transaction tries to set the lease of a key to a non-existent lease
	// Returns ErrIndexNotMonotonic if the update index is not greater than the last update index
	// Returns mvcc.ErrRevisionTooHigh if the revision specified in a query doesn't exist
	// Returns mvcc.ErrCompacted if the revision specified in a query has been compacted
	Txn(ctx context.Context, txn ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error)
	// Compact compacts the history up to this revision.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	// Returns ErrIndexNotMonotonic if the update index is not greater than the last update index
	// Returns mvcc.ErrNoRevisions if there are no revisions
	// Returns mvcc.ErrRevisionTooHigh if the revision specified doesn't exist
	// Returns mvcc.ErrCompacted if the revision specified has been compacted
	Compact(ctx context.Context, revision int64) error
	// CreateLease create a lease with the given TTL in this replica store.
	//
	// Returns ErrClosed if it is called after the store is closed
	// Returns ErrNoSuchReplicaStore if the replica store does not exist
	// Returns ErrIndexNotMonotonic if the update index is not greater than the last update index
	CreateLease(ctx context.Context, ttl int64) (ptarmiganpb.Lease, error)
	// RevokeLease deletes the lease with this ID from the replica store
	// and deletes any keys associated with this lease. It creates a new
	// revision whose changeset includes the deleted keys.
	//
	// Returns ErrClosed if it is called after the store is closed.
	// Returns ErrNoSuchReplicaStore if the replica store does not exist.
	// Returns ErrIndexNotMonotonic if the update index is not greater than the last update index
	RevokeLease(ctx context.Context, id int64) error
}
