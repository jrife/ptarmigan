package storage

import (
	"fmt"

	"github.com/jrife/ptarmigan/raft"
	"github.com/jrife/ptarmigan/state_machine"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/plugins"
	storage_raft "github.com/jrife/ptarmigan/storage/raft"
	raft_storage_class_kv "github.com/jrife/ptarmigan/storage/raft/storage_classes/kv"
	wal_storage_class_kv "github.com/jrife/ptarmigan/storage/wal/storage_classes/kv"

	storage_state_machine "github.com/jrife/ptarmigan/storage/state_machine"
	state_machine_storage_class_kv "github.com/jrife/ptarmigan/storage/state_machine/storage_classes/kv"

	"github.com/jrife/ptarmigan/storage/wal"
)

var (
	BucketRaftStores         = []byte{0}
	BucketStateMachineStores = []byte{1}
	BucketRaftSnapshotWAL    = []byte{2}
)

// StorageProvider describes an interface for
// provisioning durable storage drivers for
// different components.
type StorageProvider interface {
	// Raft returns a RaftStore instance for the specified
	// raft instance. This function should automatically
	// provision the storage if it doesn't exist yet for this
	// raft instance.
	Raft(raft.RaftSpec) (storage_raft.RaftStore, error)
	// StateMachine returns a StateMachineStore instance for
	// the specified state machine instance. This function
	// should automatically provision the storage if it doesn't
	// exist yet for this state machine instance. This function
	// must return a StateMachineStore implementation matching
	// the state machine's StorageClass. Different storage classes
	// provide different interfaces
	StateMachine(state_machine.StateMachineSpec) (storage_state_machine.StateMachineStore, error)
	// WAL returns a RaftSnapshotWAL instance. It should provision
	// the storage if it doesn't exist yet. It should return the same
	// WAL every time.
	WAL() (wal.RaftSnapshotWAL, error)
}

// SingleKVStorageProvider is an implementation of
// StorageProvider built around a single KV store
// instance. All storage objects it returns are
// based on the same KV store.
type SingleKVStorageProvider struct {
	kvStore      kv.Store
	stateMachine kv.SubStore
	raft         kv.SubStore
	wal          kv.SubStore
}

func NewSingleKVStorageProvider(kvStorePlugin string, kvStoreOptions kv.PluginOptions) (*SingleKVStorageProvider, error) {
	kvPluginManager := plugins.NewKVPluginManager()
	plugin := kvPluginManager.Plugin(kvStorePlugin)

	if plugin == nil {
		return nil, fmt.Errorf("%s is not a recognized KV store plugin", kvStorePlugin)
	}

	kvStore, err := plugin.NewStore(kvStoreOptions)

	if err != nil {
		return nil, fmt.Errorf("Could not create new %s store with options %+v: %s", kvStorePlugin, kvStoreOptions, err.Error())
	}

	storageProvider := &SingleKVStorageProvider{
		kvStore: kvStore,
	}

	if store, err := storageProvider.ensureSubStore(kvStore, BucketRaftStores); err != nil {
		return nil, fmt.Errorf("Could not ensure the existence of the raft sub-store: %s", err.Error())
	} else {
		storageProvider.raft = store
	}

	if store, err := storageProvider.ensureSubStore(kvStore, BucketStateMachineStores); err != nil {
		return nil, fmt.Errorf("Could not ensure the existence of the state machine sub-store: %s", err.Error())
	} else {
		storageProvider.stateMachine = store
	}

	if store, err := storageProvider.ensureSubStore(kvStore, BucketRaftSnapshotWAL); err != nil {
		return nil, fmt.Errorf("Could not ensure the existence of the WAL sub-store: %s", err.Error())
	} else {
		storageProvider.wal = store
	}

	return storageProvider, nil
}

func (storageProvider *SingleKVStorageProvider) ensureSubStore(parent kv.SubStore, child []byte) (kv.SubStore, error) {
	transaction, err := parent.Begin(true)

	if err != nil {
		return nil, fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	defer transaction.Rollback()

	_, err = transaction.Root().CreateBucket(child)

	if err != nil {
		return nil, fmt.Errorf("Could not create bucket %v: %s", child, err.Error())
	}

	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("Could not commit transaction: %s", err.Error())
	}

	return parent.Namespace(child), nil
}

func (storageProvider *SingleKVStorageProvider) Raft(spec raft.RaftSpec) (storage_raft.RaftStore, error) {
	store, err := storageProvider.ensureSubStore(storageProvider.raft, []byte(spec.ID))

	if err != nil {
		return nil, fmt.Errorf("Could not ensure the existence of the state machine sub-store for raft instance %s: %s", spec.ID, err.Error())
	}

	return &raft_storage_class_kv.KVRaftStore{
		Store: store,
	}, nil
}

func (storageProvider *SingleKVStorageProvider) StateMachine(spec state_machine.StateMachineSpec) (storage_state_machine.StateMachineStore, error) {
	switch spec.StorageClass {
	case state_machine_storage_class_kv.StorageClassName:
		store, err := storageProvider.ensureSubStore(storageProvider.stateMachine, []byte(spec.ID))

		if err != nil {
			return nil, fmt.Errorf("Could not ensure the existence of the state machine sub-store for state machine %s: %s", spec.ID, err.Error())
		}

		return &state_machine_storage_class_kv.KVStateMachineStore{
			Store: store,
		}, nil
	default:
		return nil, fmt.Errorf("%s is not a recognized storage class", spec.StorageClass)
	}
}

func (storageProvider *SingleKVStorageProvider) WAL() (wal.RaftSnapshotWAL, error) {
	return &wal_storage_class_kv.KVRaftSnapshotWAL{
		Store: storageProvider.wal,
	}, nil
}
