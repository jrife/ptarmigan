package wal

import (
	"errors"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/raft"
)

var ENoSuchSnapshot = errors.New("Snapshot does not exist for this raft ID")

type RaftSnapshotWALStorageClass interface {
	Name() string
	RaftSnapshotWAL() RaftSnapshotWAL
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
	ForEach(func(raft.RaftID, raftpb.Snapshot) error) error
}

// Other than snapshotting, the raft storage is fairly self-contained and
// acts sort of like a WAL for the state machine storage itself.
