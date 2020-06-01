package storage

import (
	"fmt"
	"io"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/storage/mvcc"

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
func (replicaStore *replicaStore) RaftStatus() (ptarmiganpb.RaftStatus, error) {
	var raftStatus ptarmiganpb.RaftStatus
	var err error

	replicaStore.view(mvcc.RevisionNewest, raftStatusNs, func(view mvcc.View) error {
		raftStatus, err = replicaStore.raftStatus(view)

		return err
	})

	return raftStatus, err
}

func (replicaStore *replicaStore) raftStatus(view mvcc.View) (ptarmiganpb.RaftStatus, error) {
	raftStatusKvIter, err := view.Keys(keys.All().Eq(raftStatusKey), kv.SortOrderAsc)

	if err != nil {
		return ptarmiganpb.RaftStatus{}, fmt.Errorf("could not retrieve raft status key from view: %s", err)
	}

	raftStatusKv, err := kv.Keys(raftStatusKvIter, 1)

	if err != nil {
		return ptarmiganpb.RaftStatus{}, fmt.Errorf("could not retrieve raft status key from view: %s", err)
	}

	if len(raftStatusKv) == 0 {
		return ptarmiganpb.RaftStatus{}, nil
	}

	var raftStatus ptarmiganpb.RaftStatus
	if err := raftStatus.Unmarshal(raftStatusKv[1][1]); err != nil {
		return ptarmiganpb.RaftStatus{}, fmt.Errorf("could not unmarshal raft status from %#v: %s", raftStatusKv[1][1], err)
	}

	return raftStatus, nil
}

func (replicaStore *replicaStore) applyRaftCommand(raftStatus ptarmiganpb.RaftStatus, ns []byte, revise func(revision mvcc.Revision) error) error {
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

func (replicaStore *replicaStore) compareAndSetRaftStatus(revision mvcc.Revision, raftStatus ptarmiganpb.RaftStatus) error {
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
	transaction, err := replicaStore.partition.Begin(false)

	if err != nil {
		return fmt.Errorf("could not begin mvcc transaction: %s", err)
	}

	view, err := transaction.View(revision)

	if err != nil {
		return fmt.Errorf("could not create view at %d: %s", revision, err)
	}

	defer transaction.Rollback()

	return fn(mvcc.NamespaceView(view, ns))
}

func (replicaStore *replicaStore) update(fn func(transaction mvcc.Transaction) error) error {
	transaction, err := replicaStore.partition.Begin(true)

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
func (replicaStore *replicaStore) CreateLease(raftStatus ptarmiganpb.RaftStatus, ttl int64) (ptarmiganpb.Lease, error) {
	var lease ptarmiganpb.Lease = ptarmiganpb.Lease{ID: int64(raftStatus.RaftIndex), GrantedTTL: ttl}

	err := replicaStore.applyRaftCommand(raftStatus, leasesNs, func(revision mvcc.Revision) error {
		if err := leasesMap(revision).Put(leasesKey(lease.ID), &lease); err != nil {
			return fmt.Errorf("could not put lease %d: %s", lease.ID, err)
		}

		return nil
	})

	if err != nil {
		return ptarmiganpb.Lease{}, err
	}

	return lease, nil
}

// Leases implements ReplicaStore.Leases
func (replicaStore *replicaStore) Leases() ([]ptarmiganpb.Lease, error) {
	var result []ptarmiganpb.Lease

	err := replicaStore.view(mvcc.RevisionNewest, leasesNs, func(view mvcc.View) error {
		kvs, err := leasesMapReader(view).Keys(keys.All(), kv.SortOrderAsc)

		if err != nil {
			return fmt.Errorf("could not list leases: %s", err)
		}

		for kvs.Next() {
			result = append(result, kvs.Value().(ptarmiganpb.Lease))
		}

		if kvs.Error() != nil {
			return fmt.Errorf("iteration error: %s", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetLease implements ReplicaStore.GetLease
func (replicaStore *replicaStore) GetLease(id int64) (ptarmiganpb.Lease, error) {
	var lease ptarmiganpb.Lease

	err := replicaStore.view(mvcc.RevisionNewest, leasesNs, func(view mvcc.View) error {
		l, err := leasesMapReader(view).Get(leasesKey(id))

		if err != nil {
			return fmt.Errorf("could not list leases: %s", err)
		}

		if l == nil {
			return ErrLeaseNotFound
		}

		lease = l.(ptarmiganpb.Lease)

		return nil
	})

	if err != nil {
		return ptarmiganpb.Lease{}, err
	}

	return lease, nil
}

// RevokeLease implements ReplicaStore.RevokeLease
func (replicaStore *replicaStore) RevokeLease(raftStatus ptarmiganpb.RaftStatus, id int64) error {
	return replicaStore.applyRaftCommand(raftStatus, leasesNs, func(revision mvcc.Revision) error {
		if err := leasesMap(revision).Delete(leasesKey(id)); err != nil {
			return fmt.Errorf("could not delete lease %d: %s", id, err)
		}

		return nil
	})
}

// Query implements ReplicaStore.Query
func (replicaStore *replicaStore) Query(q ptarmiganpb.KVQueryRequest) (ptarmiganpb.KVQueryResponse, error) {
	var response ptarmiganpb.KVQueryResponse

	err := replicaStore.view(q.Revision, kvsNs, func(mvccView mvcc.View) error {
		var err error
		response, err = query(mvccView, q)

		return err
	})

	return response, err
}

// Compact implements ReplicaStore.Compact
func (replicaStore *replicaStore) Compact(revision int64) error {
	return replicaStore.update(func(transaction mvcc.Transaction) error {
		return transaction.Compact(revision)
	})
}

// Changes implements ReplicaStore.Changes
func (replicaStore *replicaStore) Changes(watch ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error) {
	var response []ptarmiganpb.Event

	err := replicaStore.view(watch.Start.Revision, kvsNs, func(mvccView mvcc.View) error {
		var err error
		response, err = changes(mvccView, watch.Start.Key, limit, watch.PrevKv)

		return err
	})

	return response, err
}

// ApplySnapshot implements ReplicaStore.ApplySnapshot
func (replicaStore *replicaStore) ApplySnapshot(snap io.Reader) error {
	return replicaStore.partition.ApplySnapshot(snap)
}

// Snapshot implements ReplicaStore.Snapshot
func (replicaStore *replicaStore) Snapshot() (io.ReadCloser, error) {
	return replicaStore.partition.Snapshot()
}
