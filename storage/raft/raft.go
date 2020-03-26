package raft

import (
	etcd_raft "github.com/coreos/etcd/raft"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/raft"
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
