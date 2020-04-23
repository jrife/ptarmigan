package mvcc

import (
	"fmt"
	"io"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/kv"
	"go.uber.org/zap"
)

var (
	ReplicaMetadataBucket = []byte{0}
	ReplicaKVBucket       = []byte{1}
	MetadataKey           = []byte("metadata")
)

var _ IReplicaStore = (*ReplicaStore)(nil)

// ReplicaStore implements IReplicaStore. It
// contains various functions for accesing storage
// for a single replica representing an independent
// partition.
type ReplicaStore struct {
	name    string
	kvStore kv.SubStore
	logger  *zap.Logger
}

// Name returns the name of this replica store
func (replicaStore *ReplicaStore) Name() string {
	return replicaStore.name
}

// Create initializes this replica store with some metadata.
// It does nothing if the replica store already exists.
func (replicaStore *ReplicaStore) Create(metadata []byte) error {
	txn, err := replicaStore.kvStore.Begin(true)

	if err != nil {
		return fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer txn.Rollback()

	if err := txn.Root().Create(); err != nil {
		return fmt.Errorf("could not create bucket %s: %s", replicaStore.name, err.Error())
	}

	if err := txn.Root().Bucket(ReplicaMetadataBucket).Create(); err != nil {
		return fmt.Errorf("could not create metadata bucket: %s", err.Error())
	}

	if err := txn.Root().Bucket(ReplicaKVBucket).Create(); err != nil {
		return fmt.Errorf("could not create kv bucket: %s", err.Error())
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %s", err.Error())
	}

	return nil
}

// Delete deletes this replica store. It does nothing if the
// replica store doesn't exist.
func (replicaStore *ReplicaStore) Delete() error {
	txn, err := replicaStore.kvStore.Begin(true)

	if err != nil {
		return fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer txn.Rollback()

	if err := txn.Root().Purge(); err != nil {
		return fmt.Errorf("could not purge bucket %s: %s", replicaStore.name, err.Error())
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %s", err.Error())
	}

	return nil
}

// Metadata retrieves the metadata associated with this replica store
func (replicaStore *ReplicaStore) Metadata() ([]byte, error) {
	txn, err := replicaStore.kvStore.Begin(false)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer txn.Rollback()

	metadata, err := txn.Root().Bucket(ReplicaMetadataBucket).Get(MetadataKey)

	if err != nil {
		return nil, fmt.Errorf("could not retrieve metadata: %s", err.Error())
	}

	return metadata, nil
}

// RaftStatus returns the latest raft status for this replica store.
// Each update operation passes in a raft status containing the raft
// index and raft term associated with that update.
func (replicaStore *ReplicaStore) RaftStatus() (flockpb.RaftStatus, error) {
	return flockpb.RaftStatus{}, nil
}

// CreateLease create a lease with the given TTL in this replica store.
func (replicaStore *ReplicaStore) CreateLease(raftStatus flockpb.RaftStatus, ttl int64) (flockpb.Lease, error) {
	return flockpb.Lease{}, nil
}

// Leases lists all leases stored in this replica store
func (replicaStore *ReplicaStore) Leases() ([]flockpb.Lease, error) {
	return nil, nil
}

// GetLease reads the lease with this ID out of the replica store
func (replicaStore *ReplicaStore) GetLease(id int64) (flockpb.Lease, error) {
	return flockpb.Lease{}, nil
}

// RevokeLease deletes the lease with this ID from the replica store
// and deletes any keys associated with this lease. It creates a new
// revision whose changeset includes the deleted keys.
func (replicaStore *ReplicaStore) RevokeLease(raftStatus flockpb.RaftStatus, id int64) error {
	return nil
}

// Compact compacts the history up to this revision.
func (replicaStore *ReplicaStore) Compact(raftStatus flockpb.RaftStatus, revision int64) error {
	return nil
}

// View returns a view of the store at some revision. It returns
// ErrCompacted if the requested revision is too old and has been
// compacted away. It returns ErrRevisionTooHigh if the requested
// revision is higher than the newest revision.
func (replicaStore *ReplicaStore) View(revision int64) (IView, error) {
	return nil, nil
}

// NewRevision lets a consumer build a new revision.
func (replicaStore *ReplicaStore) NewRevision(raftStatus flockpb.RaftStatus) (IRevision, error) {
	return nil, nil
}

// ApplySnapshot completely replaces the contents of this replica
// store with those in this snapshot.
func (replicaStore *ReplicaStore) ApplySnapshot(snap io.Reader) error {
	return nil
}

// Snapshot takes a snapshot of this replica store's current state
// and encode it as a byte stream. It must be compatible with
// ApplySnapshot() such that its return value could be applied
// to ApplySnapshot() in order to replicate its state elsewhere.
func (replicaStore *ReplicaStore) Snapshot() (io.Reader, error) {
	return nil, nil
}
