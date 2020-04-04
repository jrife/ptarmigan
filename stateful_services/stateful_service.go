package stateful_services

import (
	"io"

	"github.com/coreos/etcd/raft/raftpb"
)

// Name
// StatefulServiceProvider
//
// Keyspace??
// - Namespace
// - Dataspace
// - Store
//

// Store - A namespace for partitions. Each store may have a different replication factor, or distribution settings
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
	CreateStore()
	DeleteStore()
	ListStores()
	ListReplicas() ([]StatefulServiceReplicaSpec, error)
	CreateReplica(replica StatefulServiceReplicaSpec) error
	DeleteReplica(replica StatefulServiceReplicaSpec) error
	StepReplica(replica StatefulServiceReplicaSpec, message raftpb.Entry) error
	ApplyReplicaSnapshot(replica StatefulServiceReplicaSpec, snap io.Reader) error
	ReplicaSnapshot(replica StatefulServiceReplicaSpec) (io.Reader, error)
	ReplicaLastAppliedIndex(replica StatefulServiceReplicaSpec) (uint64, error)
}
