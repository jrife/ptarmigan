package storage

import (
	"context"

	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/storage/mvcc"
	"github.com/jrife/flock/utils/log"
	"go.uber.org/zap"
)

var _ Store = (*store)(nil)

// StoreConfig contains configuration
// for a store
type StoreConfig struct {
	Logger *zap.Logger
	Store  mvcc.Store
}

// Store implements IStore. It is the
// top object for accessing storage for
// a flock node
type store struct {
	logger *zap.Logger
	store  mvcc.Store
}

// New creates an instance of Store backed by an mvcc store
func New(config StoreConfig) Store {
	store := &store{logger: config.Logger}

	if store.logger == nil {
		store.logger = zap.L()
	}

	store.store = config.Store

	return store
}

// ReplicaStores implements Store.ReplicaStores
func (store *store) ReplicaStores(ctx context.Context, start string, limit int) ([]string, error) {
	logger := log.WithContext(ctx, store.logger).With(zap.String("operation", "ReplicaStores"))
	logger.Debug("start ReplicaStores()", zap.String("start", start), zap.Int("limit", limit))

	partitions, err := store.store.Partitions(keys.All().Gt([]byte(start)), limit)

	if err != nil {
		err = wrapError("could not list partitions from mvcc store", err)

		logger.Debug("error", zap.Error(err))

		return nil, err
	}

	replicaStores := make([]string, len(partitions))

	for i, partition := range partitions {
		replicaStores[i] = string(partition)
	}

	logger.Debug("return from ReplicaStores()", zap.Strings("return", replicaStores))

	return replicaStores, nil
}

// ReplicaStore implements Store.ReplicaStore
func (store *store) ReplicaStore(name string) ReplicaStore {
	return &replicaStore{
		name:      name,
		partition: store.store.Partition([]byte(name)),
		logger:    store.logger.With(zap.String("replica_store", name)),
	}
}

// Close implements Store.Close
func (store *store) Close() error {
	return store.store.Close()
}

// Purge implements Store.Purge
func (store *store) Purge() error {
	return store.store.Delete()
}
