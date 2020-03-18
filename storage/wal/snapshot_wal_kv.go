package wal

import (
	"errors"
	"fmt"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/raft"
	"github.com/jrife/ptarmigan/storage"
	"github.com/jrife/ptarmigan/storage/kv"
)

var ENoSuchSnapshot = errors.New("Snapshot does not exist for this raft ID")

type KeySmartBucketCodec struct {
}

func (codec *KeySmartBucketCodec) Marshal(v interface{}) ([]byte, error) {
	raftID, ok := v.(raft.RaftID)

	if !ok {
		panic("Must be a raft ID")
	}

	return []byte(string(raftID)), nil
}

func (codec *KeySmartBucketCodec) Unmarshal(v []byte) (interface{}, error) {
	return raft.RaftID(string(v)), nil
}

type ValueSmartBucketCodec struct {
}

func (codec *ValueSmartBucketCodec) Marshal(v interface{}) ([]byte, error) {
	snap, ok := v.(raftpb.Snapshot)

	if !ok {
		panic("Must be a raft snapshot")
	}

	return snap.Marshal()
}

func (codec *ValueSmartBucketCodec) Unmarshal(v []byte) (interface{}, error) {
	var snap raftpb.Snapshot

	if err := snap.Unmarshal(v); err != nil {
		return nil, err
	}

	return snap, nil
}

var _ storage.RaftSnapshotWAL = (*KVRaftSnapshotWAL)(nil)

func NewKVRaftSnapshotWAL(kvStore kv.Store) *KVRaftSnapshotWAL {
	return &KVRaftSnapshotWAL{
		kvStore: &kv.SubStore{
			Store: kvStore,
			BucketAddress: [][]byte{
				[]byte(kv.BucketRaftSnapshotWAL),
			},
			SmartBucketBuilder: func(bucket kv.Bucket) *kv.SmartBucket {
				return &kv.SmartBucket{
					Bucket:     bucket,
					KeyCodec:   &KeySmartBucketCodec{},
					ValueCodec: &ValueSmartBucketCodec{},
				}
			},
		},
	}
}

type KVRaftSnapshotWAL struct {
	kvStore *kv.SubStore
}

func (raftSnapshotWAL *KVRaftSnapshotWAL) Append(raftID raft.RaftID, snap raftpb.Snapshot) error {
	if err := raftSnapshotWAL.kvStore.Put(raftID, snap); err != nil {
		return fmt.Errorf("Could not append snapshot for raft %s to WAL: %s", raftID, err.Error())
	}

	return nil
}

func (raftSnapshotWAL *KVRaftSnapshotWAL) Delete(raftID raft.RaftID) error {
	if err := raftSnapshotWAL.kvStore.Delete(raftID); err != nil {
		return fmt.Errorf("Could not delete snapshot for raft %s from WAL: %s", raftID, err.Error())
	}

	return nil
}

func (raftSnapshotWAL *KVRaftSnapshotWAL) Get(raftID raft.RaftID) (raftpb.Snapshot, error) {
	snap, err := raftSnapshotWAL.kvStore.Get(raftID)

	if err == kv.ENoSuchKey {
		return raftpb.Snapshot{}, ENoSuchSnapshot
	} else if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("Could not delete snapshot for raft %s from WAL: %s", raftID, err.Error())
	}

	return snap.(raftpb.Snapshot), nil
}

func (raftSnapshotWAL *KVRaftSnapshotWAL) ForEach(fn func(raft.RaftID, raftpb.Snapshot) error) error {
	return raftSnapshotWAL.kvStore.ForEach(func(key interface{}, value interface{}) error {
		return fn(key.(raft.RaftID), value.(raftpb.Snapshot))
	})
}
