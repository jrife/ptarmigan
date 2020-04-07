package stateful_services

import (
	"io"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/transport/ptarmiganpb"
)

// Name
// StatefulServiceProvider
//
// Keyspace??
// - Namespace
// - Dataspace
// - Store
//

// A StatefulServiceProvider could be a local implementation or
// a remote service. Local implementations can use our already established
// state machine storage drivers. External implementations in theory
// could use anything to store their data, a local file, a remote filesystem,
// even another database. While this service is "stateful", I want to avoid
// nodes in the statefulservice provider from needing to coordinate amongst
// themselves, letting them work independently while the coordination is
// driven from the core (raft management). The advantage of an imperative
// interface is the avoidance of repetitive logic
type StatefulServiceHost interface {
	ListReplicas() ([]ptarmiganpb.Replica, error)
	CreateReplica(replica ptarmiganpb.Replica) error
	DeleteReplica(replica ptarmiganpb.Replica) error
	StepReplica(replica string, message raftpb.Entry) error
	ApplyReplicaSnapshot(replica string, snap io.Reader) error
	ReplicaSnapshot(replica string) (io.Reader, error)
	ReplicaLastAppliedIndex(replica string) (uint64, error)
}
