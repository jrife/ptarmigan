package nop

import (
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/storage/state_machine"
)

// Nop provides a state machine implemenation that doesn't do anything
type Nop struct {
}

func (nop *Nop) Init(store state_machine.StateMachineStore) error {
	return nil
}

func (nop *Nop) Step(entry raftpb.Entry) ([]byte, error) {
	return nil, nil
}
