package raft

import (
	"context"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

// etc'd raft implementation allows progress on the state machine
// even when ready handlers are blocked. RawNode is mutated by
// a single goroutine. In our implementation we may need to use
// mutexes or something of that sort to ensure mutual exclusion
type Raft interface {
	ID() RaftID
	Tick()
	Propose(ctx context.Context, data []byte) error
	ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error
	Step(ctx context.Context, msg raftpb.Message) error
	Advance()
	ApplyConfChange(cc raftpb.ConfChange) *raftpb.ConfState
	HasReady() bool
	Ready() raft.Ready
}

type RaftSpec struct {
	ID string
}
