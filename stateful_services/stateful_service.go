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
	GetStorageClass() (ptarmiganpb.StorageClass, error)
	Replicas() ([]ptarmiganpb.Replica, error)
	GetReplica(replicaName string) (ptarmiganpb.Replica, error)
	CreateReplica(replica ptarmiganpb.Replica) error
	DeleteReplica(replicaName string) error
	StepReplica(replicaName string, message raftpb.Entry) (uint64, error)
	ApplySnapshot(replicaName string, snap io.Reader) error
	Snapshot(replicaName string) (io.Reader, error)
	LastAppliedIndex(replicaName string) (uint64, error)
}

// LocalStatefulServiceHost implements StatefulServiceHost
// ExternalStatefulServiceHost implements StatefulServiceHost
// Resolve replica host by looking up using master state (possibly cache): replica.status.storage_host
// StatefulServiceHostResolver returns a StatefulServiceHost suitable for this replica
// If the replica is hosted locally it will be a local implementation of StatefulServiceHost
// Otherwise it's an instance of ExternalStatefulServiceHost, a client that talks to that host remotely
type StatefulServiceHostPool interface {
	StatefulServiceHost(host string) (StatefulServiceHost, error)
}
