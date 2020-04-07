package stateful_services

import (
	"io"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/transport/ptarmiganpb"
)

// StatefulServiceHost asdf
type StatefulServiceHost interface {
	// StorageClasses returns a list of storage class providers for this host
	StorageClasses() []StorageClassProvider
	// StorageClass returns a storage class provider by name
	StorageClass(name string) StorageClassProvider
}

type StorageClassProvider interface {
	// Get returns the serializable representation of this storage class
	Get() (ptarmiganpb.StorageClass, error)
	// Replicas returns the list of replicas that exist on this host for this storage class
	Replicas() ([]ptarmiganpb.Replica, error)
	// Replica returns a replica handle for the replica with the given name
	Replica(name string) Replica
}

type Replica interface {
	Get() (ptarmiganpb.StorageClass, error)
	Create() error
	Delete() error
	Step(message raftpb.Entry) error
	ApplySnapshot(snap io.Reader) error
	Snapshot() (io.Reader, error)
	LastAppliedIndex() (uint64, error)
}

// LocalStatefulServiceHost implements StatefulServiceHost
// ExternalStatefulServiceHost implements StatefulServiceHost
// Resolve replica host by looking up using master state (possibly cache): replica.status.storage_host
// StatefulServiceHostResolver returns a StatefulServiceHost suitable for this replica
// If the replica is hosted locally it will be a local implementation of StatefulServiceHost
// Otherwise it's an instance of ExternalStatefulServiceHost, a client that talks to that host remotely
type StatefulServiceHostResolver interface {
	StatefulServiceHost(replicaName string) (StatefulServiceHost, error)
}
