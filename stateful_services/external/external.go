package stateful_services

import (
	"io"
	"net"

	"github.com/coreos/etcd/raft/raftpb"
)

// Normally there is one service provider for
// each service type per node. The scheduling
// If we had one instance of an externalserviceprovider for
// each external service registered with the cluster,
// how does the implementation determine which node to send
// requests to? This isn't a problem with built-in stateful service implementations.
// They just talk to local storage.
type ExternalServiceProvider struct {
	ReplicaHostResolver func(replica StatefulServiceReplicaSpec) (net.Addr, error)
}

func (serviceProvider *ExternalServiceProvider) CreateReplica(replica StatefulServiceReplicaSpec) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) DeleteReplica(replica StatefulServiceReplicaSpec) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) StepReplica(replica StatefulServiceReplicaSpec, message raftpb.Entry) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) ApplyReplicaSnapshot(replica StatefulServiceReplicaSpec, snap io.Reader) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) ReplicaSnapshot(replica StatefulServiceReplicaSpec) (io.Reader, error) {
	return nil, nil
}

func (serviceProvider *ExternalServiceProvider) ReplicaLastAppliedIndex(replica StatefulServiceReplicaSpec) (uint64, error) {
	return 0, nil
}
