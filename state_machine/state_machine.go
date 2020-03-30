package state_machine

import (
	"context"
	"io"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/state_machine/state_machine_pb"
	"github.com/jrife/ptarmigan/storage/state_machine"
)

// A StatefulServiceProvider could be a local implementation or
// a remote service.
type StatefulServiceProvider interface {
	CreateKeyspace()
	DeleteKeyspace()
	// Keyspace - A namespace for partitions. Each keyspace may have a different
	// replication factor, or distribution settings
	// Partition - Represents a single partition of our keyspace backed by a single raft group
	// Replica - A replica of our partition.
	// Each replica of the raft group corresponds with one replica in the state machine partition
	Step(keyspace, partition, replica, message string) error
	ApplySnapshot(keyspace, partition, replica, snap io.Reader) error
	Snapshot(keyspace, partition, replica) (io.Reader, error)
}

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
