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
	var raftStatus flockpb.RaftStatus
	var err error

	replicaStore.view(mvcc.RevisionNewest, raftStatusNs, func(view mvcc.View) error {
		raftStatus, err = replicaStore.raftStatus(view)

		return err
	})

	return raftStatus, err
}

func (replicaStore *replicaStore) raftStatus(view mvcc.View) (flockpb.RaftStatus, error) {
	raftStatusKv, err := view.Keys(kv.Gte(raftStatusKey), kv.Lte(raftStatusKey), 1, mvcc.SortOrderAsc)

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

func (replicaStore *replicaStore) applyRaftCommand(raftStatus flockpb.RaftStatus, ns []byte, revise func(revision mvcc.Revision) error) error {
	return replicaStore.revise(nil, func(revision mvcc.Revision) error {
		if err := replicaStore.compareAndSetRaftStatus(mvcc.NamespaceRevision(revision, raftStatusNs), raftStatus); err != nil {
			return fmt.Errorf("could not CAS raft status: %s", err)
		}

		if err := revise(mvcc.NamespaceRevision(revision, ns)); err != nil {
			return fmt.Errorf("could not apply new revision: %s", err)
		}

		return nil
	})
}

func (replicaStore *replicaStore) compareAndSetRaftStatus(revision mvcc.Revision, raftStatus flockpb.RaftStatus) error {
	currentRaftStatus, err := replicaStore.raftStatus(revision)

	if err != nil {
		return err
	}

	if raftStatus.RaftIndex <= currentRaftStatus.RaftIndex {
		return fmt.Errorf("consistency violation: raft index %d is not greater than current raft index %d: ", raftStatus.RaftIndex, currentRaftStatus.RaftIndex)
	}

	marshaledRaftStatus, err := raftStatus.Marshal()

	if err != nil {
		return fmt.Errorf("could not marshal raft status: %s", err)
	}

	if err := revision.Put(raftStatusKey, marshaledRaftStatus); err != nil {
		return fmt.Errorf("could not update raft status key: %s", err)
	}

	return nil
}

func (replicaStore *replicaStore) revise(ns []byte, fn func(revision mvcc.Revision) error) error {
	return replicaStore.update(func(transaction mvcc.Transaction) error {
		revision, err := transaction.NewRevision()

		if err != nil {

			return fmt.Errorf("could not create new revision: %s", err)
		}

		return fn(mvcc.NamespaceRevision(revision, ns))
	})
}

func (replicaStore *replicaStore) view(revision int64, ns []byte, fn func(view mvcc.View) error) error {
	view, err := replicaStore.partition.View(revision)

	if err != nil {
		return fmt.Errorf("could not create view at %d: %s", revision, err)
	}

	defer view.Close()

	return fn(mvcc.NamespaceView(view, ns))
}

func (replicaStore *replicaStore) update(fn func(transaction mvcc.Transaction) error) error {
	transaction, err := replicaStore.partition.Transaction()

	if err != nil {
		return fmt.Errorf("could not begin mvcc transaction: %s", err)
	}

	defer transaction.Rollback()

	if err := fn(transaction); err != nil {
		return err
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("could not commit mvcc transaction: %s", err)
	}

	return nil
}

// CreateLease implements ReplicaStore.CreateLease
func (replicaStore *replicaStore) CreateLease(raftStatus flockpb.RaftStatus, ttl int64) (flockpb.Lease, error) {
	var lease flockpb.Lease = flockpb.Lease{ID: int64(raftStatus.RaftIndex), GrantedTTL: ttl}

	err := replicaStore.applyRaftCommand(raftStatus, leasesNs, func(revision mvcc.Revision) error {
		if err := leasesRevision(revision).Put(leasesKey(lease.ID), &lease); err != nil {
			return fmt.Errorf("could not put lease %d: %s", lease.ID, err)
		}

		return nil
	})

	if err != nil {
		return flockpb.Lease{}, err
	}

	return lease, nil
}

// Leases implements ReplicaStore.Leases
func (replicaStore *replicaStore) Leases() ([]flockpb.Lease, error) {
	var result []flockpb.Lease

	err := replicaStore.view(mvcc.RevisionNewest, leasesNs, func(view mvcc.View) error {
		kvs, err := leasesView(view).Keys(nil, nil, -1, mvcc.SortOrderAsc)

		if err != nil {
			return fmt.Errorf("could not list leases: %s", err)
		}

		result = leases(kvs)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetLease implements ReplicaStore.GetLease
func (replicaStore *replicaStore) GetLease(id int64) (flockpb.Lease, error) {
	var lease flockpb.Lease

	err := replicaStore.view(mvcc.RevisionNewest, leasesNs, func(view mvcc.View) error {
		kvs, err := leasesView(view).Keys(gte(leasesKey(id)), lte(leasesKey(id)), -1, mvcc.SortOrderAsc)

		if err != nil {
			return fmt.Errorf("could not list leases: %s", err)
		}

		if len(kvs) == 0 {
			return fmt.Errorf("lease %d not found", id)
		}

		lease = leases(kvs)[0]

		return nil
	})

	if err != nil {
		return flockpb.Lease{}, err
	}

	return lease, nil
}

// RevokeLease implements ReplicaStore.RevokeLease
func (replicaStore *replicaStore) RevokeLease(raftStatus flockpb.RaftStatus, id int64) error {
	return replicaStore.applyRaftCommand(raftStatus, leasesNs, func(revision mvcc.Revision) error {
		if err := leasesRevision(revision).Delete(leasesKey(id)); err != nil {
			return fmt.Errorf("could not delete lease %d: %s", id, err)
		}

		return nil
	})
}

// Compact implements ReplicaStore.Compact
func (replicaStore *replicaStore) Compact(revision int64) error {
	return replicaStore.update(func(transaction mvcc.Transaction) error {
		return transaction.Compact(revision)
	})
}

// View implements ReplicaStore.View
func (replicaStore *replicaStore) View(revision int64) (View, error) {
	mvccView, err := replicaStore.partition.View(revision)

	if err != nil {
		return nil, fmt.Errorf("could not get view at revision %d: %s", revision, err)
	}

	return &view{
		view: kvsView(mvcc.NamespaceView(mvccView, kvsNs)),
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
			view: kvsView(mvcc.NamespaceView(mvccRevision, kvsNs)),
		},
		transaction: mvccTxn,
		revision:    kvsRevision(mvcc.NamespaceRevision(mvccRevision, kvsNs)),
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
