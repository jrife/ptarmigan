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
	err := replicaStore.partition.Create(metadata)

	return wrapError("could not create replica store", err)
}

// Delete implements ReplicaStore.Delete
func (replicaStore *replicaStore) Delete(ctx context.Context) error {
	err := replicaStore.partition.Delete()

	return wrapError("could not delete replica store", err)
}

// Metadata implements ReplicaStore.Metadata
func (replicaStore *replicaStore) Metadata(ctx context.Context) ([]byte, error) {
	metadata, err := replicaStore.partition.Metadata()

	return metadata, wrapError("could not retrieve metadata", err)
}

// Index implements ReplicaStore.Index
func (replicaStore *replicaStore) Index(ctx context.Context) (uint64, error) {
	logger := log.WithContext(ctx, replicaStore.logger).With(zap.String("operation", "Index"))
	logger.Debug("start")

	var index uint64

	err := replicaStore.read(func(transaction mvcc.Transaction) error {
		i, err := replicaStore.updateIndex(kv.NamespaceMap(transaction.Flat(), updateIndexNs))

		if err == nil {
			index = i
		}

		return err
	})

	logger.Debug("return", zap.Uint64("index", index), zap.Error(err))

	if err != nil {
		return 0, wrapError("could not retrieve index", err)
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
	err := replicaStore.update(func(transaction mvcc.Transaction) error {
		if err := replicaStore.compareAndSetUpdateIndex(kv.NamespaceMap(transaction.Flat(), updateIndexNs), index); err != nil {
			if err == ErrIndexNotMonotonic {
				return err
			}

			return wrapError("could not CAS update index", err)
		}

		if err := update(transaction); err != nil {
			return wrapError("could not apply update", err)
		}

		return nil
	})

	if err == ErrIndexNotMonotonic {
		return err
	}

	return wrapError("could not begin mvcc transaction", err)
}

func (replicaStore *replicaStore) compareAndSetUpdateIndex(updateIndexMap kv.Map, index uint64) error {
	currentUpdateIndex, err := replicaStore.updateIndex(updateIndexMap)

	if err != nil {
		return err
	}

	if index <= currentUpdateIndex {
		return ErrIndexNotMonotonic
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
		return err
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
		return err
	}

	defer transaction.Rollback()

	return fn(transaction)
}

// Leases implements ReplicaStore.Leases
func (replicaStore *replicaStore) Leases(ctx context.Context) ([]ptarmiganpb.Lease, error) {
	logger := log.WithContext(ctx, replicaStore.logger).With(zap.String("operation", "Leases"))
	logger.Debug("start")

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

	logger.Debug("return", zap.Any("leases", result), zap.Error(err))

	if err != nil {
		return nil, wrapError("could not get leases", err)
	}

	return result, nil
}

// GetLease implements ReplicaStore.GetLease
func (replicaStore *replicaStore) GetLease(ctx context.Context, id int64) (ptarmiganpb.Lease, error) {
	logger := log.WithContext(ctx, replicaStore.logger).With(zap.String("operation", "GetLease"))
	logger.Debug("start", zap.Int64("id", id))

	var lease ptarmiganpb.Lease

	err := replicaStore.read(func(transaction mvcc.Transaction) error {
		var err error
		lease, err = replicaStore.getLease(logger, transaction, id)
		return err
	})

	logger.Debug("return", zap.Any("lease", lease), zap.Error(err))

	if err != nil {
		if err != ErrNoSuchLease {
			err = wrapError("could not get lease", err)
		}

		return ptarmiganpb.Lease{}, err
	}

	return lease, nil
}

func (replicaStore *replicaStore) getLease(logger *zap.Logger, transaction mvcc.Transaction, id int64) (ptarmiganpb.Lease, error) {
	l, err := leasesMapReader(kv.NamespaceMap(transaction.Flat(), leasesNs)).Get(leasesKey(id))

	if err != nil {
		return ptarmiganpb.Lease{}, fmt.Errorf("could not get lease: %s", err)
	}

	if l == nil {
		return ptarmiganpb.Lease{}, ErrNoSuchLease
	}

	return l.(ptarmiganpb.Lease), nil
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

	return response, wrapError("could not perform query", err)
}

// Changes implements ReplicaStore.Changes
func (replicaStore *replicaStore) Changes(ctx context.Context, watch ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error) {
	logger := log.WithContext(ctx, replicaStore.logger).With(zap.String("operation", "Changes"))
	logger.Debug("start", zap.Any("watch", watch))

	var response []ptarmiganpb.Event = []ptarmiganpb.Event{}

	if watch.Start == nil {
		watch.Start = &ptarmiganpb.KVWatchCursor{Revision: mvcc.RevisionOldest}
	}

	err := replicaStore.read(func(transaction mvcc.Transaction) error {
		view, err := transaction.View(watch.Start.Revision)

		if err == mvcc.ErrNoRevisions && watch.Start.Revision == mvcc.RevisionOldest {
			response = []ptarmiganpb.Event{}

			return nil
		} else if err != nil {
			return err
		}

		for err == nil && (limit <= 0 || len(response) < limit) {
			response, err = changes(response, view, watch, limit)

			if err != nil {
				continue
			}

			// clear after the first revision since now we just advance one by one
			watch.Start = nil
			view, err = transaction.View(view.Revision() + 1)

			if err != nil {
				continue
			}
		}

		if err == mvcc.ErrRevisionTooHigh {
			err = nil
		}

		return err
	})

	logger.Debug("return", zap.Any("response", response), zap.Error(err))

	if err != nil {
		return []ptarmiganpb.Event{}, wrapError("could not get changes", err)
	}

	return response, err
}

func (replicaStore *replicaStore) NewestRevision(ctx context.Context) (int64, error) {
	var revision int64

	err := replicaStore.view(mvcc.RevisionNewest, func(mvccView mvcc.View) error {
		revision = mvccView.Revision()
		return nil
	})

	if err != nil {
		return 0, wrapError("could not retrieve newest revision", err)
	}

	return revision, nil
}

func (replicaStore *replicaStore) OldestRevision(ctx context.Context) (int64, error) {
	var revision int64

	err := replicaStore.view(mvcc.RevisionOldest, func(mvccView mvcc.View) error {
		revision = mvccView.Revision()
		return nil
	})

	if err != nil {
		return 0, wrapError("could not retrieve oldest revision", err)
	}

	return revision, nil
}

// ApplySnapshot implements ReplicaStore.ApplySnapshot
func (replicaStore *replicaStore) ApplySnapshot(ctx context.Context, snap io.Reader) error {
	err := replicaStore.partition.ApplySnapshot(ctx, snap)

	return wrapError("could not apply snapshot", err)
}

// Snapshot implements ReplicaStore.Snapshot
func (replicaStore *replicaStore) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	snap, err := replicaStore.partition.Snapshot(ctx)

	return snap, wrapError("could not take snapshot", err)
}

type update struct {
	replicaStore *replicaStore
	index        uint64
	logger       *zap.Logger
}

// RevokeLease implements Update.RevokeLease
func (update *update) RevokeLease(ctx context.Context, id int64) error {
	logger := log.WithContext(ctx, update.logger).With(zap.String("operation", "RevokeLease"))
	logger.Debug("start", zap.Int64("id", id))

	err := update.replicaStore.apply(update.index, func(transaction mvcc.Transaction) error {
		v, err := leasesMap(kv.NamespaceMap(transaction.Flat(), leasesNs)).Get(leasesKey(id))

		if err != nil {
			return fmt.Errorf("could not get lease %d: %s", id, err)
		}

		if v == nil || (v.(ptarmiganpb.Lease) == ptarmiganpb.Lease{}) {
			logger.Debug("lease does not exist")

			return nil
		}

		if err := leasesMap(kv.NamespaceMap(transaction.Flat(), leasesNs)).Delete(leasesKey(id)); err != nil {
			return fmt.Errorf("could not delete lease %d: %s", id, err)
		}

		view, err := transaction.View(mvcc.RevisionNewest)

		if err != nil && err != mvcc.ErrNoRevisions {
			return fmt.Errorf("could not create view of latest revision: %s", err)
		}

		var revision mvcc.Revision
		_, revision, err = update.executeTxnOp(update.logger, transaction, view, nil, ptarmiganpb.KVTxnRequest{
			Success: []*ptarmiganpb.KVRequestOp{
				{
					Request: &ptarmiganpb.KVRequestOp_RequestDelete{
						RequestDelete: &ptarmiganpb.KVDeleteRequest{
							Selection: &ptarmiganpb.KVSelection{
								Lease: id,
							},
						},
					},
				},
			},
		})

		if revision != nil {
			logger.Debug("new revision created", zap.Int64("revision", revision.Revision()))
		}

		return err
	})

	logger.Debug("return", zap.Error(err))

	return err
}

// CreateLease implements Update.CreateLease
func (update *update) CreateLease(ctx context.Context, ttl int64) (ptarmiganpb.Lease, error) {
	logger := log.WithContext(ctx, update.logger).With(zap.String("operation", "CreateLease"))
	logger.Debug("start", zap.Int64("ttl", ttl))

	var lease ptarmiganpb.Lease = ptarmiganpb.Lease{GrantedTTL: ttl}

	err := update.replicaStore.apply(update.index, func(transaction mvcc.Transaction) error {
		v, err := kv.NamespaceMap(transaction.Flat(), metaNs).Get(metaLeaseIDKey)

		if err != nil {
			return fmt.Errorf("could not retrieve lease ID key: %s", err)
		}

		var id int64

		if v != nil {
			id = int64(binary.BigEndian.Uint64(v))
		}

		id++
		v = make([]byte, 8)

		binary.BigEndian.PutUint64(v, uint64(id))
		lease.ID = id

		if err := kv.NamespaceMap(transaction.Flat(), metaNs).Put(metaLeaseIDKey, v); err != nil {
			return fmt.Errorf("could not update lease ID key: %s", err)
		}

		if err := leasesMap(kv.NamespaceMap(transaction.Flat(), leasesNs)).Put(leasesKey(id), &lease); err != nil {
			return fmt.Errorf("could not put lease %d: %s", lease.ID, err)
		}

		return nil
	})

	logger.Debug("return", zap.Any("lease", lease), zap.Error(err))

	if err != nil {
		return ptarmiganpb.Lease{}, err
	}

	return lease, nil
}

// Compact implements Update.Compact
func (update *update) Compact(ctx context.Context, revision int64) error {
	logger := log.WithContext(ctx, update.logger).With(zap.String("operation", "Compact"), zap.Int64("revision", revision))
	logger.Debug("start")

	var compactErr error

	err := update.replicaStore.apply(update.index, func(transaction mvcc.Transaction) error {
		compactErr = transaction.Compact(revision)

		// These failures indicate a precondition
		// failure rather than a disk storage
		// failure or some such thing that requires
		// us to abort the transaction. The update
		// index should still be recorded as this
		// update was "applied" it just has no effect
		switch compactErr {
		case mvcc.ErrCompacted:
			fallthrough
		case mvcc.ErrRevisionTooHigh:
			fallthrough
		case mvcc.ErrNoRevisions:
			logger.Debug("precondition failed for compaction", zap.Error(compactErr))

			// Returning nil ensures that the transaction will be committed and record
			// the update index even if the net effect is a no-op
			return nil
		}

		return compactErr
	})

	if err == nil {
		err = compactErr
	}

	logger.Debug("return", zap.Error(err))

	return err
}
