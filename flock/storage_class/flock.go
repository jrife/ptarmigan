package stateful_services

import (
	"io"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/stateful_services"
	"github.com/jrife/ptarmigan/transport/ptarmiganpb"
)

var _ stateful_services.StorageClassProvider = (*FlockStorageClassProvider)(nil)

type FlockStorageClassProvider struct {
}

func (provider *FlockStorageClassProvider) GetStorageClass() (ptarmiganpb.StorageClass, error) {
	return ptarmiganpb.StorageClass{
		Metadata: ptarmiganpb.ObjectMetadata{
			Name: "flock",
		},
		Spec:   ptarmiganpb.StorageClassSpec{},
		Status: ptarmiganpb.StorageClassStatus{},
	}, nil
}

func (provider *FlockStorageClassProvider) Replicas() ([]ptarmiganpb.Replica, error) {
	return nil, nil
}

func (provider *FlockStorageClassProvider) GetReplica(replicaName string) (ptarmiganpb.Replica, error) {
	return ptarmiganpb.Replica{}, nil
}

func (provider *FlockStorageClassProvider) CreateReplica(replica ptarmiganpb.Replica) error {
	return nil
}

func (provider *FlockStorageClassProvider) DeleteReplica(replicaName string) error {
	return nil
}

func (provider *FlockStorageClassProvider) StepReplica(replicaName string, message raftpb.Entry) (uint64, error) {
	return nil
}

func (provider *FlockStorageClassProvider) ApplySnapshot(replicaName string, snap io.Reader) error {
	return nil
}

func (provider *FlockStorageClassProvider) Snapshot(replicaName string) (io.Reader, error) {
	return nil, nil
}

func (provider *FlockStorageClassProvider) LastAppliedIndex(replicaName string) (uint64, error) {
	return 0, nil
}
