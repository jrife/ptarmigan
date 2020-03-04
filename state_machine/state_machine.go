package state_machine

import (
	"context"

	"github.com/jrife/ptarmigan/state_machine/state_machine_pb"
)

type StateMachine interface {
	ID() StateMachineID
	Step(command state_machine_pb.Command) (updateHint []byte)
	ApplySnapshot()
}

// State machine interface:
// Interface: Request/Response
// Under The Hood: Proposal -> Commit
type CommandQueue interface {
	SubmitCommand(ctx context.Context, command []byte)
	OnCommand(command state_machine_pb.Command)
}
