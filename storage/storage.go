package storage

import (
	"io"

	etcd_raft "github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/raft"

	"github.com/jrife/ptarmigan/state_machine"
)

// I've been struggling to think of a general enough
// storage interface so that we can adapt it to meet
// the needs of many state machines. The most important
// invariant is consistency at rest. That is to say,
// at any given moment the state on disk must represent
// a consistent state, where consistent is defined by
// some application specific parameters. This may require
// a write-ahead-log as a first-class abstraction of our API
//
// CONSISTENCY AT REST: If the program crashes at any time,
// a consistent state is recoverable on restart.
//
// The challenges I face are presenting the following
// through a series of interfaces that are flexible:
//   1) Reading a consistent snapshot
//   2) Applying a consistent snapshot atomically (no partial updates)
//   3) Knowing the highest entry applied to a state machine
//   4) Allowing flexible storage options for a state machine
// Consequences of having a snapshot applied to a state machine store
// without having applied the corresponding update to the raft
// store? This could cause an inconsistency? 2PC or something like that
// to prevent problems? WAL?

type SnapshotterCb func() (raftpb.Snapshot, error)

type RaftStorageClass interface {
	// Just a factory method
	RaftStore(raftID raft.RaftID) RaftStore
}

type RaftStore interface {
	etcd_raft.Storage
	Init() error
	Purge() error
	OnSnapshot(SnapshotterCb)
	SetHardState(st raftpb.HardState) error
	Append(entries []raftpb.Entry) error
}

type StateMachineStorageClass interface {
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
	ApplySnapshot(io.ReadCloser) error
}

// Before StateMachineStore.ApplySnapshot() is called
// RaftSnapshotWAL.Append() is called to record the intent
// to stable storage. Since StateMachineStore.ApplySnapshot()
// is atomic its results will be totally synced to stable
// storage or not at all. We want to arrive at a state
// where the raft store state matches what is in the
// state machine store before allowing new progress on
// that raft state machine. On node startup we see if there
// were any raft snapshot transactions that were interrupted.
// There are three possible cases:
// 1) An entry exists in the raft snapshot WAL for a raft instance.
//    The LastAppliedRaftIndex() of the linked state machine matches
//    the Index in the raft snapshot. This indicates that we may
//    not have finished the snapshot tranaction and still need to
//    apply the raft snapshot to the raft store. The application
//    must not allow raft machine interactions until the snapshot
//    is totally applied.
// 2) An entry exists in the raft snapshot WAL for a raft instance.
//    The LastAppliedRaftIndex() does not match the index of the
//    raft snapshot. This means the state machine snapshot was never
//    persisted. Need to delete this entry from the WAL and report
//    failure back to sender.
type RaftSnapshotWAL interface {
	Append(raft.RaftID, raftpb.Snapshot) error
	Delete(raft.RaftID) error
	Get(raft.RaftID) (raftpb.Snapshot, error)
	ForEach(func(raft.RaftID, raftpb.Snapshot)) error
}
