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

	err := replicaStore.read(func(transaction mvcc.Transaction) error {
		i, err := replicaStore.updateIndex(kv.NamespaceMap(transaction.Flat(), updateIndexNs))

		if err == nil {
			index = i
		}

		return err
	})

	if err != nil {
		return 0, err
	}

	return index, nil
}

func (replicaStore *replicaStore) Apply(index uint64) Update {
	return &update{
		replicaStore: replicaStore,
		index:        index,
		logger:       replicaStore.logger.With(zap.Uint64("update", index)),
	}
}

func (replicaStore *replicaStore) updateIndex(updateIndexMap kv.Map) (uint64, error) {
	updateIndexKvIter, err := updateIndexMap.Keys(keys.All().Eq(updateIndexKey), kv.SortOrderAsc)

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

func (replicaStore *replicaStore) apply(index uint64, update func(transaction mvcc.Transaction) error) error {
	return replicaStore.update(func(transaction mvcc.Transaction) error {
		if err := replicaStore.compareAndSetUpdateIndex(kv.NamespaceMap(transaction.Flat(), updateIndexNs), index); err != nil {
			return fmt.Errorf("could not CAS update index: %s", err)
		}

		if err := update(transaction); err != nil {
			return fmt.Errorf("could not apply update: %s", err)
		}

		return nil
	})
}

func (replicaStore *replicaStore) compareAndSetUpdateIndex(updateIndexMap kv.Map, index uint64) error {
	currentUpdateIndex, err := replicaStore.updateIndex(updateIndexMap)

	if err != nil {
		return err
	}

	if index <= currentUpdateIndex {
		return fmt.Errorf("consistency violation: update index %d is not greater than current update index %d: ", index, currentUpdateIndex)
	}

	marshaledIndex := make([]byte, 8)
	binary.BigEndian.PutUint64(marshaledIndex, index)

	if err := updateIndexMap.Put(updateIndexKey, marshaledIndex); err != nil {
		return fmt.Errorf("could not update update index key: %s", err)
	}

	return nil
}

func (replicaStore *replicaStore) view(revision int64, fn func(view mvcc.View) error) error {
	return replicaStore.read(func(transaction mvcc.Transaction) error {
		view, err := transaction.View(revision)

		if err != nil {
			return wrapError(fmt.Sprintf("could not create view at %d", revision), err)
		}

		return fn(view)
	})
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

func (replicaStore *replicaStore) read(fn func(transaction mvcc.Transaction) error) error {
	transaction, err := replicaStore.partition.Begin(false)

	if err != nil {
		return fmt.Errorf("could not begin mvcc transaction: %s", err)
	}

	defer transaction.Rollback()

	return fn(transaction)
}

// Leases implements ReplicaStore.Leases
func (replicaStore *replicaStore) Leases(ctx context.Context) ([]ptarmiganpb.Lease, error) {
	var result []ptarmiganpb.Lease = []ptarmiganpb.Lease{}

	err := replicaStore.read(func(transaction mvcc.Transaction) error {
		kvs, err := leasesMapReader(kv.NamespaceMap(transaction.Flat(), leasesNs)).Keys(keys.All(), kv.SortOrderAsc)

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

	err := replicaStore.read(func(transaction mvcc.Transaction) error {
		l, err := leasesMapReader(kv.NamespaceMap(transaction.Flat(), leasesNs)).Get(leasesKey(id))

		if err != nil {
			return fmt.Errorf("could not get lease: %s", err)
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

	err := replicaStore.view(q.Revision, func(mvccView mvcc.View) error {
		var err error

		response, err = query(logger, mvccView, q)

		return err
	})

	if err == mvcc.ErrNoRevisions && q.Revision == mvcc.RevisionNewest {
		response = ptarmiganpb.KVQueryResponse{Kvs: []*ptarmiganpb.KeyValue{}}
		err = nil
	}

	logger.Debug("return", zap.Any("response", response), zap.Error(err))

	return response, err
}

// Changes implements ReplicaStore.Changes
func (replicaStore *replicaStore) Changes(ctx context.Context, watch ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error) {
	var response []ptarmiganpb.Event

	if watch.Start == nil {
		watch.Start = &ptarmiganpb.KVWatchCursor{Revision: mvcc.RevisionOldest}
	}

	err := replicaStore.view(watch.Start.Revision, func(mvccView mvcc.View) error {
		var err error
		response, err = changes(mvccView, watch.Start.Key, limit, watch.PrevKv)

		return err
	})

	if err == mvcc.ErrNoRevisions && watch.Start.Revision == mvcc.RevisionOldest {
		response = []ptarmiganpb.Event{}
		err = nil
	}

	return response, err
}

func (replicaStore *replicaStore) NewestRevision() (int64, error) {
	var revision int64

	err := replicaStore.view(mvcc.RevisionNewest, func(mvccView mvcc.View) error {
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

	err := replicaStore.view(mvcc.RevisionOldest, func(mvccView mvcc.View) error {
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
	return update.replicaStore.apply(update.index, func(transaction mvcc.Transaction) error {
		if err := leasesMap(kv.NamespaceMap(transaction.Flat(), leasesNs)).Delete(leasesKey(id)); err != nil {
			return fmt.Errorf("could not delete lease %d: %s", id, err)
		}

		return nil
	})
}

// CreateLease implements Update.CreateLease
func (update *update) CreateLease(ctx context.Context, ttl int64) (ptarmiganpb.Lease, error) {
	var lease ptarmiganpb.Lease = ptarmiganpb.Lease{ID: int64(update.index), GrantedTTL: ttl}

	err := update.replicaStore.apply(update.index, func(transaction mvcc.Transaction) error {
		if err := leasesMap(kv.NamespaceMap(transaction.Flat(), leasesNs)).Put(leasesKey(lease.ID), &lease); err != nil {
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
	logger := log.WithContext(ctx, update.logger).With(zap.String("operation", "Compact"), zap.Int64("revision", revision))
	logger.Debug("start")

	err := update.replicaStore.apply(update.index, func(transaction mvcc.Transaction) error {
		err := transaction.Compact(revision)

		// These failures contitute a precondition
		// failure rather than a disk storage
		// failure or some such thing that requires
		// us to abort the transaction. The update
		// index should still be recorded as this
		// update was "applied" it just has no effect
		switch err {
		case mvcc.ErrCompacted:
			fallthrough
		case mvcc.ErrRevisionTooHigh:
			fallthrough
		case mvcc.ErrNoRevisions:
			logger.Debug("precondition failed for compaction", zap.Error(err))

			return nil
		}

		return err
	})

	logger.Debug("return", zap.Error(err))

	return err
}
