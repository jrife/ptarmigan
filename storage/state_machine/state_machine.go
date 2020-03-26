package state_machine

import (
	"io"

	"github.com/jrife/ptarmigan/state_machine"
)

type StateMachineStorageClass interface {
	Name() string
	// Just a factory method. We could add more stuff later
	// to this interface if we need functions that affect
	// all instances of this storage class
	StateMachineStore(stateMachineID state_machine.StateMachineID) StateMachineStore
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
type StateMachineStore interface {
	Init() error
	Purge() error
	// Keep track of the last applied raft index
	// We shouldn't need to keep track of the term
	// because we're only concerned with committed
	// indexes. Every committed index should be processed
	// by the state machine.
	LastAppliedRaftIndex() uint64
	// Create a consistent snapshot of the current state.
	// When deserialized the new instance should return
	// the same raft index.
	Snapshot() (io.ReadCloser, error)
	// Apply a consistent snapshot atomically
	// At the very least this needs to be atomic
	// (all or nothing) and the storage class
	// driver takes care of cleaning up artifacts
	// from failed snapshots
	ApplySnapshot(io.Reader) error
}
