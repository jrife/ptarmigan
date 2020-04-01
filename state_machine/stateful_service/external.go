package stateful_service

import (
	"io"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/state_machine"
)

// Normally there is one service provider for
// each service type per node. The scheduling
// If we had one instance of an externalserviceprovider for
// each external service registered with the cluster,
// how does the implementation determine which node to send
// requests to? This isn't a problem with built-in stateful service implementations.
// They just talk to local storage.
type ExternalServiceProvider struct {
}

func (serviceProvider *ExternalServiceProvider) CreateReplica(replica state_machine.StatefulServiceReplicaSpec) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) DeleteReplica(replica state_machine.StatefulServiceReplicaSpec) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) StepReplica(replica state_machine.StatefulServiceReplicaSpec, message raftpb.Entry) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) ApplyReplicaSnapshot(replica state_machine.StatefulServiceReplicaSpec, snap io.Reader) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) ReplicaSnapshot(replica state_machine.StatefulServiceReplicaSpec) (io.Reader, error) {
	return nil, nil
}

func (serviceProvider *ExternalServiceProvider) ReplicaLastAppliedIndex(replica state_machine.StatefulServiceReplicaSpec) (uint64, error) {
	return 0, nil
}
