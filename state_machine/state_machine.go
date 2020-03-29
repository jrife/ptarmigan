package state_machine

import (
	"context"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/state_machine/state_machine_pb"
	"github.com/jrife/ptarmigan/storage/state_machine"
)

type StateMachine interface {
	Init(store state_machine.StateMachineStore) error
	Step(command raftpb.Entry) ([]byte, error)
}

// State machine interface:
// Interface: Request/Response
// Under The Hood: Proposal -> Commit
type CommandQueue interface {
	SubmitCommand(ctx context.Context, command []byte)
	OnCommand(command state_machine_pb.Command)
}

type StateMachineSpec struct {
	ID           string
	StorageClass string
}
