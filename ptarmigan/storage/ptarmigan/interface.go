package ptarmigan

import (
	"errors"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/snapshot"
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

type KVStore interface {
	// Query executes a query
	Query(query ptarmiganpb.KVQueryRequest) (ptarmiganpb.KVQueryResponse, error)
	// Txn executes a transaction
	Txn(raftStatus ptarmiganpb.RaftStatus, txn ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error)
	// Compact compacts the history up to this revision.
	Compact(revision int64) error
	// Changes returns a set of change events associated with this
	// revision. Events are always sorted in ascending order by the
	// byte order of their key. Returns events whose key is > start up
	// to limit. In other words, start acts as a cursor and limit
	// limits the size of the resulting events.
	Changes(watch ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error)
}

type LeaseStore interface {
	// CreateLease create a lease with the given TTL in this replica store.
	CreateLease(raftStatus ptarmiganpb.RaftStatus, ttl int64) (ptarmiganpb.Lease, error)
	// RevokeLease deletes the lease with this ID from the replica store
	// and deletes any keys associated with this lease. It creates a new
	// revision whose changeset includes the deleted keys.
	RevokeLease(raftStatus ptarmiganpb.RaftStatus, id int64) error
	// Leases lists all leases stored in this replica store
	Leases() ([]ptarmiganpb.Lease, error)
	// GetLease reads the lease with this ID out of the replica store
	GetLease(id int64) (ptarmiganpb.Lease, error)
}

// ReplicaStore describes a storage interface for a flock replica.
type ReplicaStore interface {
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
	// RaftStatus returns the latest raft status for this store.
	// Each update operation passes in a raft status containing the raft
	// index and raft term associated with that update.
	RaftStatus() (ptarmiganpb.RaftStatus, error)
	KVStore
	LeaseStore
	// ApplySnapshot completely replaces the contents of this replica
	// store with those in this snapshot.
	snapshot.Acceptor
	// Snapshot takes a snapshot of this replica store's current state
	// and encode it as a byte stream. It must be compatible with
	// ApplySnapshot() such that its return value could be applied
	// to ApplySnapshot() in order to replicate its state elsewhere.
	snapshot.Source
}
