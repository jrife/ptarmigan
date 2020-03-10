package state_machine

import (
	"fmt"
	"io"

	"github.com/jrife/ptarmigan/state_machine"
	"github.com/jrife/ptarmigan/storage"
	"github.com/jrife/ptarmigan/storage/kv"
)

// KV Storage class - for state machines that
// prefer to use a transactional KV interface
// for their persistent storage.
type KVStateMachineStorageClass struct {
}

func (stateMachineStorageClass *KVStateMachineStorageClass) StateMachineStore(stateMachineID state_machine.StateMachineID) storage.StateMachineStore {
	return nil
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
}

func (stateMachineStore *KVStateMachineStore) Init() error {
	return nil
}

func (stateMachineStore *KVStateMachineStore) Purge() error {
	return nil
}

func (stateMachineStore *KVStateMachineStore) LastApplied() uint64 {
	return 0
}

func (stateMachineStore *KVStateMachineStore) Snapshot() (io.ReadCloser, error) {
	return nil, nil
}

func (stateMachineStore *KVStateMachineStore) ApplySnapshot(io.ReadCloser) error {
	return nil
}

func (stateMachineStore *KVStateMachineStore) Begin(writable bool) (kv.Transaction, error) {
	return nil, nil
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
