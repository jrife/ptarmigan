package state_machine

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jrife/ptarmigan/state_machine"
	"github.com/jrife/ptarmigan/storage"
	"github.com/jrife/ptarmigan/storage/kv"
)

const (
	BucketStateMachineMetadata = "metadata"
	BucketStateMachineState    = "state"
	KeyLastAppliedRaftIndex    = "raft_index"
)

// KV Storage class - for state machines that
// prefer to use a transactional KV interface
// for their persistent storage.
type KVStateMachineStorageClass struct {
	KVStore kv.Store
}

func (stateMachineStorageClass *KVStateMachineStorageClass) StateMachineStore(stateMachineID state_machine.StateMachineID) storage.StateMachineStore {
	return &KVStateMachineStore{
		kvStore: stateMachineStorageClass.KVStore.SubStore([]byte(stateMachineID)),
	}
}

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
	kvStore              kv.Store
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
	transaction, err := stateMachineStore.kvStore.Begin(false)

	if err != nil {
		return nil, fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	snapshot, err := transaction.Snapshot()

	if err != nil {
		transaction.Rollback()

		return nil, fmt.Errorf("Could not create snapshot: %s", err.Error())
	}

	return &SnapshotReader{transaction: transaction, snapshot: snapshot}, nil
}

func (stateMachineStore *KVStateMachineStore) ApplySnapshot(snapshot io.ReadCloser) error {
	transaction, err := stateMachineStore.kvStore.Begin(true)

	if err != nil {
		return fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	err = transaction.ApplySnapshot(snapshot)

	if err != nil {
		return fmt.Errorf("Could not apply snapshot: %s", err.Error())
	}

	return nil
}

// Step should only be called by one thread
func (stateMachineStore *KVStateMachineStore) Step(raftIndex uint64) (kv.Transaction, error) {
	transaction, err := stateMachineStore.kvStore.Begin(true)

	if err != nil {
		return nil, fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	metadata := transaction.Bucket([]byte(BucketStateMachineMetadata))
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

var _ io.ReadCloser = (*SnapshotReader)(nil)

// SnapshotReader ensures that the transaction
// assicated with this snapshot is closed when
// the reader is closed
type SnapshotReader struct {
	transaction kv.Transaction
	snapshot    io.ReadCloser
}

func (snapshotReader *SnapshotReader) Read(p []byte) (n int, err error) {
	return snapshotReader.snapshot.Read(p)
}

// Close closes the reader and rolls back the
// transaction associated with this snapshot
func (snapshotReader *SnapshotReader) Close() error {
	err := snapshotReader.snapshot.Close()

	if err != nil {
		return fmt.Errorf("Could not close inner snapshot reader: %s", err.Error())
	}

	err = snapshotReader.transaction.Rollback()

	if err != nil {
		return fmt.Errorf("Could not close snapshot transaction: %s", err.Error())
	}

	return nil
}
