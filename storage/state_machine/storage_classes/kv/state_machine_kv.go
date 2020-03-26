package kv

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jrife/ptarmigan/storage/kv"
)

const (
	StorageClassName           = "kv"
	BucketStateMachineMetadata = "metadata"
	BucketStateMachineState    = "state"
	KeyLastAppliedRaftIndex    = "raft_index"
)

// Each type of state machine can describe its own
// StateMachineStorage or use one that is already
// defined. In our case we'll implement a storage
// implementation that multiplexes a lot of state
// machine storages over the same transactional kv store.
// This interface is essential for ptarmigan to take
// and transfer snapshots at the right time.
// New state machine types are welcome to structure
// their persistent storage howerver they want
// as long as they register a new state machine storage driver
// and it implements this interface.
type KVStateMachineStore struct {
	Store                kv.SubStore
	lastAppliedRaftIndex uint64
}

func (stateMachineStore *KVStateMachineStore) Init() error {
	return nil
}

func (stateMachineStore *KVStateMachineStore) Purge() error {
	return nil
}

func (stateMachineStore *KVStateMachineStore) LastAppliedRaftIndex() uint64 {
	return stateMachineStore.lastAppliedRaftIndex
}

func (stateMachineStore *KVStateMachineStore) Snapshot() (io.ReadCloser, error) {
	snapshot, err := stateMachineStore.Store.Snapshot()

	if err != nil {
		snapshot.Close()

		return nil, fmt.Errorf("Could not create snapshot: %s", err.Error())
	}

	return snapshot, nil
}

func (stateMachineStore *KVStateMachineStore) ApplySnapshot(snapshot io.Reader) error {
	return stateMachineStore.Store.ApplySnapshot(snapshot)
}

// Step should only be called by one thread
func (stateMachineStore *KVStateMachineStore) Step(raftIndex uint64) (kv.Transaction, error) {
	transaction, err := stateMachineStore.Store.Begin(true)

	if err != nil {
		return nil, fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	metadata := transaction.Root().Bucket([]byte(BucketStateMachineMetadata))
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, raftIndex)

	if err := metadata.Put([]byte(KeyLastAppliedRaftIndex), encoded); err != nil {
		transaction.Rollback()

		return nil, fmt.Errorf("Could not update raft index")
	}

	transaction.OnCommit(func() {
		stateMachineStore.lastAppliedRaftIndex = raftIndex
	})

	return transaction.Namespace([]byte(BucketStateMachineState)), nil
}
