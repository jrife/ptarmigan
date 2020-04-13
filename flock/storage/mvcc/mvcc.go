package mvcc

import (
	"errors"
	"io"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
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
)

// IStore describes a storage interface
// for a flock node.
type IStore interface {
	// ReplicaStores lets a consumer iterate through
	// all the replica stores for this node. Replica
	// stores are returned in order by their name.
	// Returns a list of IReplicaStores whose name is
	// > start up to the specified limit.
	ReplicaStores(start string, limit int) ([]IReplicaStore, error)
	// ReplicaStore returns a handle to the replica
	// store with the given name. Calling methods on
	// the handle will return ErrNoSuchReplicaStore
	// if it hasn't been created.
	ReplicaStore(name string) IReplicaStore
}

// IReplicaStore describes a storage interface for a flock replica.
type IReplicaStore interface {
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
	// RaftStatus returns the latest raft status for this replica store.
	// Each update operation passes in a raft status containing the raft
	// index and raft term associated with that update.
	RaftStatus() (flockpb.RaftStatus, error)
	// CreateLease create a lease with the given TTL in this replica store.
	CreateLease(raftStatus flockpb.RaftStatus, ttl int64) (flockpb.Lease, error)
	// Leases lists all leases stored in this replica store
	Leases() ([]flockpb.Lease, error)
	// GetLease reads the lease with this ID out of the replica store
	GetLease(id int64) (flockpb.Lease, error)
	// RevokeLease deletes the lease with this ID from the replica store
	// and deletes any keys associated with this lease. It creates a new
	// revision whose changeset includes the deleted keys.
	RevokeLease(raftStatus flockpb.RaftStatus, id int64) error
	// Compact compacts the history up to this revision.
	Compact(raftStatus flockpb.RaftStatus, revision int64, metadata []byte) error
	// View returns a view of the store at some revision. It returns
	// ErrCompacted if the requested revision is too old and has been
	// compacted away. It returns ErrRevisionTooHigh if the requested
	// revision is higher than the newest revision.
	View(revision int64) (IView, error)
	// NewRevision lets a consumer build a new revision.
	NewRevision(raftStatus flockpb.RaftStatus) (IRevision, error)
	// ApplySnapshot completely replaces the contents of this replica
	// store with those in this snapshot.
	ApplySnapshot(snap io.Reader) error
	// Snapshot takes a snapshot of this replica store's current state
	// and encode it as a byte stream. It must be compatible with
	// ApplySnapshot() such that its return value could be applied
	// to ApplySnapshot() in order to replicate its state elsewhere.
	Snapshot() (io.Reader, error)
}

// IView describes a view of a replica store
// at some revision. It lets a consumer read
// the state of the keys at that revision.
type IView interface {
	// Query executes a query on this view (ignores revision in request)
	Query(query flockpb.KVQueryRequest) (flockpb.KVQueryResponse, error)
	// Get reads a key at a revision. It returns ErrNotFound if the key
	// does not exist at this revision.
	Get(key []byte) (flockpb.KeyValue, error)
	// Revision returns the revision index of this view
	Revision() (int64, error)
	// Changes returns a set of change events associated with this
	// revision. Events are always sorted in ascending order by the
	// byte order of their key. Returns events whose key is > start up
	// to limit. In other words, start acts as a cursor and limit
	// limits the size of the resulting events.
	Changes(start []byte, limit int) ([]flockpb.Event, error)
}

// IRevision lets a consumer build a new revision
type IRevision interface {
	IView
	// Put creates or updates a key.
	Put(key []byte, value []byte) error
	// Delete deletes a key. If the key doesn't
	// exist no error is returned.
	Delete(key []byte) error
	// Commit commits this revision, making its
	// effects visible.
	Commit() error
	// Abort discards this revision. Its effects
	// will not be visible to any observers.
	Abort() error
}
