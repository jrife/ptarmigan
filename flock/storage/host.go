package storage

import (
	"io"

	"github.com/jrife/ptarmigan/flock/storage/mvcc"
	"github.com/jrife/ptarmigan/transport/ptarmiganpb"
)

type FlockHost struct {
	store mvcc.IStore
}

func (host *FlockHost) Replicas() ([]ptarmiganpb.Replica, error) {
	return nil, nil
}

func (host *FlockHost) GetReplica(replicaName string) (ptarmiganpb.Replica, error) {
	return ptarmiganpb.Replica{}, nil
}

func (host *FlockHost) CreateReplica(replica ptarmiganpb.Replica) error {
	encoded, err := replica.Marshal()

	if err != nil {
		return err
	}

	return host.store.ReplicaStore(replica.Metadata.Name).Create(encoded)
}

func (host *FlockHost) DeleteReplica(replicaName string) error {
	return host.store.ReplicaStore(replicaName).Delete()
}

func (host *FlockHost) ApplySnapshot(replicaName string, snap io.Reader) error {
	return host.store.ReplicaStore(replicaName).ApplySnapshot(snap)
}

func (host *FlockHost) Snapshot(replicaName string) (io.Reader, error) {
	return host.store.ReplicaStore(replicaName).Snapshot()
}

func (host *FlockHost) LastAppliedIndex(replicaName string) (uint64, error) {
	return 0, nil
}
