package flock

import (
	"fmt"
	"io"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/mvcc"
	"go.uber.org/zap"
)

var _ ReplicaStore = (*replicaStore)(nil)

// replicaStore implements ReplicaStore
type replicaStore struct {
	name      string
	partition mvcc.Partition
	logger    *zap.Logger
}

// Name implements ReplicaStore.Name
func (replicaStore *replicaStore) Name() string {
	return replicaStore.name
}

// Create implements ReplicaStore.Create
func (replicaStore *replicaStore) Create(metadata []byte) error {
	return replicaStore.partition.Create(metadata)
}

// Delete implements ReplicaStore.Delete
func (replicaStore *replicaStore) Delete() error {
	return replicaStore.partition.Delete()
}

// Metadata implements ReplicaStore.Metadata
func (replicaStore *replicaStore) Metadata() ([]byte, error) {
	return replicaStore.partition.Metadata()
}

// RaftStatus implements ReplicaStore.RaftStatus
func (replicaStore *replicaStore) RaftStatus() (flockpb.RaftStatus, error) {
	currentView, err := replicaStore.partition.View(mvcc.RevisionNewest)

	if err != nil {
		return flockpb.RaftStatus{}, fmt.Errorf("could not retrieve raft status: %s", err)
	}

	defer currentView.Close()

	currentView = mvcc.NamespaceView(currentView, raftStatusNs)
	raftStatusKv, err := currentView.Keys(kv.Gte(raftStatusKey), kv.Lte(raftStatusKey), 1, mvcc.SortOrderAsc)

	if err != nil {
		return flockpb.RaftStatus{}, fmt.Errorf("could not retrieve raft status key from view: %s")
	}

	if len(raftStatusKv) == 0 {
		return flockpb.RaftStatus{}, fmt.Errorf("raft status key does not exist")
	}

	var raftStatus flockpb.RaftStatus
	if err := raftStatus.Unmarshal(raftStatusKv[1][1]); err != nil {
		return flockpb.RaftStatus{}, fmt.Errorf("could not unmarshal raft status from %#v: %s", raftStatusKv[1][1], err)
	}

	return raftStatus, nil
}

// CreateLease implements ReplicaStore.CreateLease
func (replicaStore *replicaStore) CreateLease(raftStatus flockpb.RaftStatus, ttl int64) (flockpb.Lease, error) {
	return flockpb.Lease{}, nil
}

// Leases implements ReplicaStore.Leases
func (replicaStore *replicaStore) Leases() ([]flockpb.Lease, error) {
	return nil, nil
}

// GetLease implements ReplicaStore.GetLease
func (replicaStore *replicaStore) GetLease(id int64) (flockpb.Lease, error) {
	return flockpb.Lease{}, nil
}

// RevokeLease implements ReplicaStore.RevokeLease
func (replicaStore *replicaStore) RevokeLease(raftStatus flockpb.RaftStatus, id int64) error {
	return nil
}

// Compact implements ReplicaStore.Compact
func (replicaStore *replicaStore) Compact(revision int64) error {
	txn, err := replicaStore.partition.Transaction()

	if err != nil {
		return fmt.Errorf("could not start mvcc transaction: %s", err)
	}

	defer txn.Rollback()

	return txn.Compact(revision)
}

// View implements ReplicaStore.View
func (replicaStore *replicaStore) View(revision int64) (View, error) {
	mvccView, err := replicaStore.partition.View(revision)

	if err != nil {
		return nil, fmt.Errorf("could not get view at revision %d: %s", revision, err)
	}

	return &view{
		view: mvcc.NamespaceView(mvccView, kvsNs),
	}, nil
}

// NewRevision implements ReplicaStore.NewRevision
func (replicaStore *replicaStore) NewRevision(raftStatus flockpb.RaftStatus) (Revision, error) {
	mvccTxn, err := replicaStore.partition.Transaction()

	if err != nil {
		return nil, fmt.Errorf("could not start mvcc transaction: %s", err.Error())
	}

	mvccRevision, err := mvccTxn.NewRevision()

	if err != nil {
		mvccTxn.Rollback()

		return nil, fmt.Errorf("could not create new revision: %s", err)
	}

	return &revision{
		view: view{
			view: mvcc.NamespaceRevision(mvccRevision, kvsNs),
		},
		transaction: mvccTxn,
	}, nil
}

// ApplySnapshot implements ReplicaStore.ApplySnapshot
func (replicaStore *replicaStore) ApplySnapshot(snap io.Reader) error {
	return replicaStore.partition.ApplySnapshot(snap)
}

// Snapshot implements ReplicaStore.Snapshot
func (replicaStore *replicaStore) Snapshot() (io.Reader, error) {
	return replicaStore.partition.Snapshot()
}
