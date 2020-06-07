package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/storage/mvcc"
	"github.com/jrife/flock/utils/log"

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
func (replicaStore *replicaStore) Create(ctx context.Context, metadata []byte) error {
	return replicaStore.partition.Create(metadata)
}

// Delete implements ReplicaStore.Delete
func (replicaStore *replicaStore) Delete(ctx context.Context) error {
	return replicaStore.partition.Delete()
}

// Metadata implements ReplicaStore.Metadata
func (replicaStore *replicaStore) Metadata(ctx context.Context) ([]byte, error) {
	return replicaStore.partition.Metadata()
}

// Index implements ReplicaStore.Index
func (replicaStore *replicaStore) Index(ctx context.Context) (uint64, error) {
	var index uint64
	var err error

	replicaStore.view(mvcc.RevisionNewest, updateIndexNs, func(view mvcc.View) error {
		index, err = replicaStore.updateIndex(view)

		return err
	})

	return index, err
}

func (replicaStore *replicaStore) Apply(index uint64) Update {
	return &update{
		replicaStore: replicaStore,
		index:        index,
		logger:       replicaStore.logger.With(zap.Uint64("update", index)),
	}
}

func (replicaStore *replicaStore) updateIndex(view mvcc.View) (uint64, error) {
	updateIndexKvIter, err := view.Keys(keys.All().Eq(updateIndexKey), kv.SortOrderAsc)

	if err != nil {
		return 0, fmt.Errorf("could not retrieve update index key from view: %s", err)
	}

	updateIndexKv, err := kv.Keys(updateIndexKvIter, 1)

	if err != nil {
		return 0, fmt.Errorf("could not retrieve update index key from view: %s", err)
	}

	if len(updateIndexKv) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(updateIndexKv[0][1]), nil
}

func (replicaStore *replicaStore) applyUpdate(index uint64, ns []byte, revise func(revision mvcc.Revision) error) error {
	return replicaStore.revise(nil, func(revision mvcc.Revision) error {
		if err := replicaStore.compareAndSetUpdateIndex(mvcc.NamespaceRevision(revision, updateIndexNs), index); err != nil {
			return fmt.Errorf("could not CAS update index: %s", err)
		}

		if err := revise(mvcc.NamespaceRevision(revision, ns)); err != nil {
			return fmt.Errorf("could not apply new revision: %s", err)
		}

		return nil
	})
}

func (replicaStore *replicaStore) compareAndSetUpdateIndex(revision mvcc.Revision, index uint64) error {
	currentUpdateIndex, err := replicaStore.updateIndex(revision)

	if err != nil {
		return err
	}

	if index <= currentUpdateIndex {
		return fmt.Errorf("consistency violation: update index %d is not greater than current update index %d: ", index, currentUpdateIndex)
	}

	marshaledRaftStatus := make([]byte, 8)
	binary.BigEndian.PutUint64(marshaledRaftStatus, currentUpdateIndex)

	if err := revision.Put(updateIndexKey, marshaledRaftStatus); err != nil {
		return fmt.Errorf("could not update update index key: %s", err)
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

	defer transaction.Rollback()

	view, err := transaction.View(revision)

	if err != nil {
		return fmt.Errorf("could not create view at %d: %s", revision, err)
	}

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

// Leases implements ReplicaStore.Leases
func (replicaStore *replicaStore) Leases(ctx context.Context) ([]ptarmiganpb.Lease, error) {
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
func (replicaStore *replicaStore) GetLease(ctx context.Context, id int64) (ptarmiganpb.Lease, error) {
	var lease ptarmiganpb.Lease

	err := replicaStore.view(mvcc.RevisionNewest, leasesNs, func(view mvcc.View) error {
		l, err := leasesMapReader(view).Get(leasesKey(id))

		if err != nil {
			return fmt.Errorf("could not list leases: %s", err)
		}

		if l == nil {
			return ErrNoSuchLease
		}

		lease = l.(ptarmiganpb.Lease)

		return nil
	})

	if err != nil {
		return ptarmiganpb.Lease{}, err
	}

	return lease, nil
}

// Query implements ReplicaStore.Query
func (replicaStore *replicaStore) Query(ctx context.Context, q ptarmiganpb.KVQueryRequest) (ptarmiganpb.KVQueryResponse, error) {
	logger := log.WithContext(ctx, replicaStore.logger).With(zap.String("operation", "Query"))
	logger.Debug("start", zap.Any("query", q))

	var response ptarmiganpb.KVQueryResponse

	err := replicaStore.view(q.Revision, kvsNs, func(mvccView mvcc.View) error {
		var err error

		response, err = query(logger, mvccView, q)

		return err
	})

	logger.Debug("return", zap.Any("response", response), zap.Error(err))

	return response, err
}

// Changes implements ReplicaStore.Changes
func (replicaStore *replicaStore) Changes(ctx context.Context, watch ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error) {
	var response []ptarmiganpb.Event

	err := replicaStore.view(watch.Start.Revision, kvsNs, func(mvccView mvcc.View) error {
		var err error
		response, err = changes(mvccView, watch.Start.Key, limit, watch.PrevKv)

		return err
	})

	return response, err
}

func (replicaStore *replicaStore) NewestRevision() (int64, error) {
	var revision int64

	err := replicaStore.view(mvcc.RevisionNewest, kvsNs, func(mvccView mvcc.View) error {
		revision = mvccView.Revision()
		return nil
	})

	if err != nil {
		return 0, err
	}

	return revision, nil
}

func (replicaStore *replicaStore) OldestRevision() (int64, error) {
	var revision int64

	err := replicaStore.view(mvcc.RevisionOldest, kvsNs, func(mvccView mvcc.View) error {
		revision = mvccView.Revision()
		return nil
	})

	if err != nil {
		return 0, err
	}

	return revision, nil
}

// ApplySnapshot implements ReplicaStore.ApplySnapshot
func (replicaStore *replicaStore) ApplySnapshot(ctx context.Context, snap io.Reader) error {
	return replicaStore.partition.ApplySnapshot(ctx, snap)
}

// Snapshot implements ReplicaStore.Snapshot
func (replicaStore *replicaStore) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return replicaStore.partition.Snapshot(ctx)
}

type update struct {
	replicaStore *replicaStore
	index        uint64
	logger       *zap.Logger
}

// RevokeLease implements Update.RevokeLease
func (update *update) RevokeLease(ctx context.Context, id int64) error {
	return update.replicaStore.applyUpdate(update.index, leasesNs, func(revision mvcc.Revision) error {
		if err := leasesMap(revision).Delete(leasesKey(id)); err != nil {
			return fmt.Errorf("could not delete lease %d: %s", id, err)
		}

		return nil
	})
}

// CreateLease implements Update.CreateLease
func (update *update) CreateLease(ctx context.Context, ttl int64) (ptarmiganpb.Lease, error) {
	var lease ptarmiganpb.Lease = ptarmiganpb.Lease{ID: int64(update.index), GrantedTTL: ttl}

	err := update.replicaStore.applyUpdate(update.index, leasesNs, func(revision mvcc.Revision) error {
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

// Compact implements Update.Compact
func (update *update) Compact(ctx context.Context, revision int64) error {
	return update.replicaStore.update(func(transaction mvcc.Transaction) error {
		return transaction.Compact(revision)
	})
}
