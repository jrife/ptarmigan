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
	ErrNoSuchReplicaStore = errors.New("No such replica store")
	// ErrLeaseNotFound is returned when an operation tries to access a key
	// that does not exist at the current revision.
	ErrLeaseNotFound = errors.New("No such lease")
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
	ReplicaStores(ctx context.Context, start string, limit int) ([]string, error)
	// ReplicaStore returns a handle to the replica
	// store with the given name. Calling methods on
	// the handle will return ErrNoSuchReplicaStore
	// if it hasn't been created.
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
	// This must only be called on a closed store.
	Purge() error
}

// ReplicaStore describes a storage interface for a ptarmigan replica.
type ReplicaStore interface {
	// Name returns the name of this replica store
	Name() string
	// Create initializes this replica store with some metadata.
	// It does nothing if the replica store already exists.
	Create(ctx context.Context, metadata []byte) error
	// Delete deletes this replica store. It does nothing if the
	// replica store doesn't exist.
	Delete(ctx context.Context) error
	// Metadata retrieves the metadata associated with this replica store
	Metadata(ctx context.Context) ([]byte, error)
	// Index returns the latest index applied to this store.
	Index(ctx context.Context) (uint64, error)
	// Apply an update with this update index
	Apply(index uint64) Update
	// Query executes a query
	Query(ctx context.Context, query ptarmiganpb.KVQueryRequest) (ptarmiganpb.KVQueryResponse, error)
	// Changes returns a set of change events associated with this
	// revision. Events are always sorted in ascending order by the
	// byte order of their key. Returns events whose key is > start up
	// to limit. In other words, start acts as a cursor and limit
	// limits the size of the resulting events.
	Changes(ctx context.Context, watch ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error)
	// Leases lists all leases stored in this replica store
	Leases(ctx context.Context) ([]ptarmiganpb.Lease, error)
	// GetLease reads the lease with this ID out of the replica store
	GetLease(ctx context.Context, id int64) (ptarmiganpb.Lease, error)
	// ApplySnapshot completely replaces the contents of this replica
	// store with those in this snapshot.
	snapshot.Acceptor
	// Snapshot takes a snapshot of this replica store's current state
	// and encode it as a byte stream. It must be compatible with
	// ApplySnapshot() such that its return value could be applied
	// to ApplySnapshot() in order to replicate its state elsewhere.
	snapshot.Source
}

// Update describes an update to a replica store
type Update interface {
	// Txn executes a transaction
	Txn(ctx context.Context, txn ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error)
	// Compact compacts the history up to this revision.
	Compact(ctx context.Context, revision int64) error
	// CreateLease create a lease with the given TTL in this replica store.
	CreateLease(ctx context.Context, ttl int64) (ptarmiganpb.Lease, error)
	// RevokeLease deletes the lease with this ID from the replica store
	// and deletes any keys associated with this lease. It creates a new
	// revision whose changeset includes the deleted keys.
	RevokeLease(ctx context.Context, id int64) error
}
