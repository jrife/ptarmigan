package flock

import (
	"errors"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/snapshot"
)

const (
	// RevisionOldest can be used in place
	// of a revision index to indicate the
	// oldest available revision.
	RevisionOldest = -1
	// RevisionNewest can be used in place
	// of a revision index to indicate the
	// newest available revision.
	RevisionNewest = -2
	// ReplicaStoreMin can be used in place
	// of a replica store name to indicate
	// the replica store with the lexicographically
	// lowest name.
	ReplicaStoreMin = ""
	// ReplicaStoreMax can be used in place
	// of a replica store name to indicate
	// the replica store with the lexicographically
	// highest name.
	ReplicaStoreMax = ""
)

var (
	// ErrNoSuchReplicaStore is returned when a replica store
	// is requested that does not exist.
	ErrNoSuchReplicaStore = errors.New("No such replica store")
	// ErrCompacted is returned when an operation cannot be completed
	// because it needs to read data that was compacted away.
	ErrCompacted = errors.New("Replica store was compacted")
	// ErrRevisionTooHigh is returned when an operation tries to access
	// a revision number that is higher than the newest revision.
	ErrRevisionTooHigh = errors.New("Requested revision number is higher than the newest revision")
	// ErrNotFound is returned when an operation tries to access a key
	// that does not exist at the current revision.
	ErrNotFound = errors.New("No such key")
	// ErrClosed is returned when an operation is attempted
	// after the store is closed.
	ErrClosed = errors.New("The store was closed")
	// ErrKeyNotFound is returned by get when the requested key is
	// not found
	ErrKeyNotFound = errors.New("Key not found")
)

var (
	leasesNs      = []byte{0}
	raftStatusNs  = []byte{1}
	kvsNs         = []byte{2}
	raftStatusKey = []byte{0}
)

// Store describes a storage interface
// for a flock node.
type Store interface {
	// ReplicaStores lets a consumer iterate through
	// all the replica stores for this node. Replica
	// stores are returned in order by their name.
	// Returns a list of ReplicaStores whose name is
	// > start up to the specified limit.
	ReplicaStores(start string, limit int) ([]string, error)
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

// RaftCommandStore contains functions that apply incoming raft
// commands to the store. It is up to the caller to ensure only
// one thread at a time call these functions, as raft commands
// must be applied in the order indicated by their index.
type RaftCommandStore interface {
	// RaftStatus returns the latest raft status for this store.
	// Each update operation passes in a raft status containing the raft
	// index and raft term associated with that update.
	RaftStatus() (flockpb.RaftStatus, error)
	// CreateLease create a lease with the given TTL in this replica store.
	CreateLease(raftStatus flockpb.RaftStatus, ttl int64) (flockpb.Lease, error)
	// RevokeLease deletes the lease with this ID from the replica store
	// and deletes any keys associated with this lease. It creates a new
	// revision whose changeset includes the deleted keys.
	RevokeLease(raftStatus flockpb.RaftStatus, id int64) error
	// NewRevision lets a consumer build a new revision.
	NewRevision(raftStatus flockpb.RaftStatus) (Revision, error)
}

// ReplicaStore describes a storage interface for a flock replica.
type ReplicaStore interface {
	RaftCommandStore
	// Name returns the name of this replica store
	Name() string
	// Create initializes this replica store with some metadata.
	// It does nothing if the replica store already exists.
	Create(metadata []byte) error
	// Delete deletes this replica store. It does nothing if the
	// replica store doesn't exist.
	Delete() error
	// Metadata retrieves the metadata associated with this replica store
	Metadata() ([]byte, error)
	// Leases lists all leases stored in this replica store
	Leases() ([]flockpb.Lease, error)
	// GetLease reads the lease with this ID out of the replica store
	GetLease(id int64) (flockpb.Lease, error)
	// Compact compacts the history up to this revision.
	Compact(revision int64) error
	// View returns a view of the store at some revision. It returns
	// ErrCompacted if the requested revision is too old and has been
	// compacted away. It returns ErrRevisionTooHigh if the requested
	// revision is higher than the newest revision.
	View(revision int64) (View, error)
	// ApplySnapshot completely replaces the contents of this replica
	// store with those in this snapshot.
	snapshot.Acceptor
	// Snapshot takes a snapshot of this replica store's current state
	// and encode it as a byte stream. It must be compatible with
	// ApplySnapshot() such that its return value could be applied
	// to ApplySnapshot() in order to replicate its state elsewhere.
	snapshot.Source
}

// View describes a view of a replica store
// at some revision. It lets a consumer read
// the state of the keys at that revision.
type View interface {
	// Query executes a query on this view (ignores revision in request)
	Query(query flockpb.KVQueryRequest) (flockpb.KVQueryResponse, error)
	// Get reads a key at a revision. It returns ErrNotFound if the key
	// does not exist at this revision.
	Get(key []byte) (flockpb.KeyValue, error)
	// Revision returns the revision index of this view
	Revision() int64
	// Changes returns a set of change events associated with this
	// revision. Events are always sorted in ascending order by the
	// byte order of their key. Returns events whose key is > start up
	// to limit. In other words, start acts as a cursor and limit
	// limits the size of the resulting events.
	Changes(start []byte, limit int, includePrev bool) ([]flockpb.Event, error)
	// Close must be called when a consumer is done with the view
	Close() error
}

// Revision lets a consumer build a new revision
type Revision interface {
	View
	// Put creates or updates a key.
	Put(req flockpb.KVPutRequest) (flockpb.KVPutResponse, error)
	// Delete deletes a key. If the key doesn't
	// exist no error is returned.
	Delete(req flockpb.KVDeleteRequest) (flockpb.KVDeleteResponse, error)
	// Commit commits this revision, making its
	// effects visible.
	Commit() error
	// Rollback discards this revision. Its effects
	// will not be visible to any observers.
	Rollback() error
}
