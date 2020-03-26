package kv

import (
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/raft"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/wal"
)

var _ wal.RaftSnapshotWAL = (*KVRaftSnapshotWAL)(nil)

type KVRaftSnapshotWAL struct {
	Store kv.SubStore
}

func (raftSnapshotWAL *KVRaftSnapshotWAL) Append(raftID raft.RaftID, snap raftpb.Snapshot) error {
	return nil
}

func (raftSnapshotWAL *KVRaftSnapshotWAL) Delete(raftID raft.RaftID) error {
	return nil
}

func (raftSnapshotWAL *KVRaftSnapshotWAL) Get(raftID raft.RaftID) (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, nil
}

func (raftSnapshotWAL *KVRaftSnapshotWAL) ForEach(fn func(raft.RaftID, raftpb.Snapshot) error) error {
	return nil
}
