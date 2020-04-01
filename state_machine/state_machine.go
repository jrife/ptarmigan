package state_machine

import (
	"context"
	"io"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/state_machine/state_machine_pb"
	"github.com/jrife/ptarmigan/storage/state_machine"
)

// Name
// StatefulServiceProvider
//
// Keyspace??
// - Namespace
// - Dataspace
// - Store
//

// Keyspace - A namespace for partitions. Each keyspace may have a different
// replication factor, or distribution settings
// Partition - Represents a single partition of our keyspace backed by a single raft group
// Replica - A replica of our partition.
// Each replica of the raft group corresponds with one replica in the state machine partition
// Placeholders
type StatefulServiceReplicaSpec struct {
	Store     string
	Partition string
	ID        string
}

// A StatefulServiceProvider could be a local implementation or
// a remote service. Local implementations can use our already established
// state machine storage drivers. External implementations in theory
// could use anything to store their data, a local file, a remote filesystem,
// even another database. While this service is "stateful", I want to avoid
// nodes in the statefulservice provider from needing to coordinate amongst
// themselves, letting them work independently while the coordination is
// driven from the core (raft management). The advantage of an imperative
// interface is the avoidance of repetitive logic
type StatefulServiceProvider interface {
	CreateReplica(replica StatefulServiceReplicaSpec) error
	DeleteReplica(replica StatefulServiceReplicaSpec) error
	StepReplica(replica StatefulServiceReplicaSpec, message raftpb.Entry) error
	ApplyReplicaSnapshot(replica StatefulServiceReplicaSpec, snap io.Reader) error
	ReplicaSnapshot(replica StatefulServiceReplicaSpec) (io.Reader, error)
	ReplicaLastAppliedIndex(replica StatefulServiceReplicaSpec) (uint64, error)
}

// Side-note:
// we'll need a service like this on each node:
// Obviously we might be missing some necessary startup parameters
// We can build up this service first, and a basic stateful service provider
// Then once we have a stable basis for creating and deleting multiple raft
// groups hosted on our nodes we can start to build out the declarative
// control interface on top of an existing stateful service (coordination service) (bootstrap)
// We'll need a service that exposes some functions like those described in this interface.
// Maybe we should have a container type that links them like Replica?
// Notice that this interface is per-node. It's mostly for internal use
// The main user-facing interface would be the declarative model where a controller makes these
// calls to each node to effect change.
// 1. StatefulServiceProvider - Ptarmigan - A coordination service
// 2. A Master keyspace of Ptarmigan
// 3. There exists an HA controller that moves actual state toward desired state stored in the Master keyspace
//    It uses these imperative calls to move cluster towards desired state.
type RaftService interface {
	CreateRaftGroupReplica(raftGroup, replica string, peers []string) error
	DeleteRaftGroupReplica(raftGroup, replica string)
	LinkRaftGroupReplicaToKeyspacePartitionReplica(raftGroup string, replica string, provider string, keyspace string, replica string)
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
