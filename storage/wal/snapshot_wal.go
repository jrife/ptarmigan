package wal

import (
	"github.com/jrife/ptarmigan/raft"
	"github.com/jrife/ptarmigan/storage"
	"go.etcd.io/etcd/raft/raftpb"
)

var _ storage.RaftSnapshotWAL = (*RaftSnapshotWAL)(nil)

type RaftSnapshotWAL struct {
}

func (raftSnapshotWAL *RaftSnapshotWAL) Append(raft.RaftID, raftpb.Snapshot) error {
	return nil
}

func (raftSnapshotWAL *RaftSnapshotWAL) Delete(raft.RaftID) error {
	return nil
}

func (raftSnapshotWAL *RaftSnapshotWAL) Get(raft.RaftID) (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, nil
}

func (raftSnapshotWAL *RaftSnapshotWAL) ForEach(fn func(raft.RaftID, raftpb.Snapshot)) error {
	return nil
}
