package mvcc

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/jrife/ptarmigan/storage/kv"
)

var (
	ReplicasBucket = []byte{0}
)

var _ IStore = (*Store)(nil)

// StoreConfig contains configuration
// for a store
type StoreConfig struct {
	KV     kv.SubStore
	Logger *zap.Logger
}

// Store implements IStore. It is the
// top object for accessing storage for
// a flock node
type Store struct {
	kvStore kv.SubStore
	logger  *zap.Logger
}

// NewStore creates an instance of IStore backed by a single
// kv store
func NewStore(config StoreConfig) (*Store, error) {
	store := &Store{kvStore: config.KV, logger: config.Logger}

	if store.logger == nil {
		store.logger = zap.L()
	}

	if err := store.ensureBuckets(); err != nil {
		store.logger.Error("could not ensure buckets", zap.Error(err))
		return nil, fmt.Errorf("could not ensure buckets: %s", err.Error())
	}

	return store, nil
}

func (store *Store) ensureBuckets() error {
	transaction, err := store.kvStore.Begin(true)

	if err != nil {
		return fmt.Errorf("Could not start transaction to ensure existence of required buckets: %s", err.Error())
	}

	err = transaction.Root().Bucket(ReplicasBucket).Create()

	if err != nil {
		return fmt.Errorf("Could not ensure existence of replicas bucket: %s", err.Error())
	}

	return nil
}

// ReplicaStores lets a consumer iterate through
// all the replica stores for this node. Replica
// stores are returned in order by their name.
// Returns a list of IReplicaStores whose name is
// > start up to the specified limit.
func (store *Store) ReplicaStores(ctx context.Context, start string, limit int) ([]IReplicaStore, error) {
	return nil, nil
}

// ReplicaStore returns a handle to the replica
// store with the given name. Calling methods on
// the handle will return ErrNoSuchReplicaStore
// if it hasn't been created.
func (store *Store) ReplicaStore(name string) IReplicaStore {
	return &ReplicaStore{
		name:    name,
		kvStore: store.kvStore.Namespace([]byte(ReplicasBucket)).Namespace([]byte(name)),
		logger:  store.logger,
	}
}

// Close closes the store. Function calls to any I/O objects
// descended from this store occurring after Close returns
// must have no effect and return ErrClosed. Close must not
// return until all concurrent I/O operations have concluded.
// Operations started after the call to Close is started but
// before it returns may proceed normally or may return ErrClosed.
// If they return ErrClosed they must have no effect. Close may
// return an error to indicate any problems that occurred during
// shutdown.
func (store *Store) Close() error {
	return nil
}

// Purge deletes all persistent data associated with this store.
// This must only be called on a closed store.
func (store *Store) Purge() error {
	return nil
}
