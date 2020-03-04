package replica

import (
	"github.com/jrife/ptarmigan/raft"
	"github.com/jrife/ptarmigan/state_machine"
)

type Replica interface {
	StateMachine() state_machine.StateMachine
	Raft() raft.Raft
}
