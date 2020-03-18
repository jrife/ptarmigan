package raft

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/raft"

	"github.com/jrife/ptarmigan/storage"
	"github.com/jrife/ptarmigan/storage/kv"
	etcd_raft "go.etcd.io/etcd/raft"
)

const (
	BucketRaftLogMetadata = "metadata"
	BucketRaftLogEntries  = "entries"
	KeyHardState          = "hard_state"
	KeyConfState          = "conf_state"
)

// Raft storage on top of a transactional
// KV store. Slow but quick to implement at least
type KVRaftStorageClass struct {
	KVStore kv.Store
}

func (raftStorageClass *KVRaftStorageClass) RaftStore(raftID raft.RaftID) storage.RaftStore {
	return &KVRaftStore{
		kvStore: raftStorageClass.KVStore.SubStore([]byte(raftID)),
	}
}

// A RaftStore based on a kv store
type KVRaftStore struct {
	mu             sync.Mutex
	raftHardState  raftpb.HardState
	raftConfState  raftpb.ConfState
	raftLastIndex  uint64
	raftFirstIndex uint64
	snapshotter    storage.SnapshotterCb
	kvStore        kv.Store
	name           []byte
}

func (raftStore *KVRaftStore) myBucket(transaction kv.Transaction) kv.Bucket {
	return nil
}

func (raftStore *KVRaftStore) Init() error {
	return nil
}

func (raftStore *KVRaftStore) Purge() error {
	return nil
}

func (raftStore *KVRaftStore) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	raftStore.mu.Lock()
	defer raftStore.mu.Unlock()

	return raftStore.raftHardState, raftStore.raftConfState, nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (raftStore *KVRaftStore) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if hi < lo {
		panic("Hi must not be less than lo")
	}

	transaction, err := raftStore.kvStore.Begin(false)

	if err != nil {
		return nil, err
	}

	defer transaction.Rollback()

	raftLog := &RaftLogBucket{Bucket: raftStore.myBucket(transaction).Bucket([]byte("TODO"))}
	entries := raftLog.Entries()

	if !entries.Seek(lo) {
		if entries.Error() == nil {
			return nil, etcd_raft.ErrUnavailable
		}

		return nil, fmt.Errorf("Could not find entry at index %d: %s", lo, entries.Error().Error())
	}

	if lo < entries.Index() {
		return nil, etcd_raft.ErrCompacted
	} else if lo != entries.Index() {
		return nil, fmt.Errorf("Expected lo to equal first entry index but it doens't: lo != entries.Index(): %d != %d ", lo, entries.Index())
	}

	entry := entries.Entry()
	size := entry.Size()
	result := make([]raftpb.Entry, 0, hi-lo+1)
	result = append(result, entry)

	for entries.Next() {
		if entries.Entry().Index != entry.Index+1 {
			return nil, fmt.Errorf("Entries don't have consecutive indexes: %d follows %d", entries.Entry().Index, entry.Index)
		}

		entry = entries.Entry()
		size += entry.Size()

		if uint64(size) > maxSize {
			break
		}

		result = append(result, entry)
	}

	if entries.Error() != nil {
		return nil, entries.Error()
	}

	return result, nil
}

func (raftStore *KVRaftStore) Term(i uint64) (uint64, error) {
	// Requires read

	return 0, nil
}

func (raftStore *KVRaftStore) LastIndex() (uint64, error) {
	raftStore.mu.Lock()
	defer raftStore.mu.Unlock()

	return raftStore.raftLastIndex, nil
}

func (raftStore *KVRaftStore) FirstIndex() (uint64, error) {
	raftStore.mu.Lock()
	defer raftStore.mu.Unlock()

	return raftStore.raftFirstIndex, nil
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
//
// NOTE: Maybe it's possible for this to work if we just return the "current"
// snapshot/current state so we don't have to go "back in time" in the state machine
// history? From the looks of it, this function could simply return the current state
// of our system rather than some previous state as implied by the comment above.
//
// This function is called by the raft state machine when it is a leader and detects
// that a follower is too far behind to catch up by sending it deltas (entries). It
// calls this function to obtain a snapshot that can be used as the basis for sending
// that replica more entries. Obtaining a consistent snapshot requires that we send
// a view of the state machine storage that matches up with the current snapshot
// metadata
// How to coordinate?
// - We could bubble up the need for a snapshot to the mediator through a series of callbacks
//   and have the mediator choose the appropriate time to ensure consistency between the Index,
//   Term, ConfState, and the StateMachine state that will be streamed. Invert control back up
//   IoC might be our best bet. Callback here, invokes callback in the ptarmigan.raft.Raft interface
//   implementation.
func (raftStore *KVRaftStore) Snapshot() (raftpb.Snapshot, error) {
	if raftStore.snapshotter == nil {
		return raftpb.Snapshot{}, etcd_raft.ErrSnapshotTemporarilyUnavailable
	}

	return raftStore.snapshotter()
}

func (raftStore *KVRaftStore) OnSnapshot(cb storage.SnapshotterCb) {
	raftStore.snapshotter = cb
}

func (raftStore *KVRaftStore) SetHardState(st raftpb.HardState) error {
	return nil
}

func (raftStore *KVRaftStore) Append(entries []raftpb.Entry) error {
	return nil
}

type RaftLogBucket struct {
	kv.Bucket
}

func (bucket *RaftLogBucket) SetHardState(st raftpb.HardState) error {
	marshaledHardState, err := st.Marshal()

	if err != nil {
		return err
	}

	bucket.putMetadata([]byte(KeyHardState), marshaledHardState)

	return nil
}

func (bucket *RaftLogBucket) SetConfState(cs raftpb.ConfState) error {
	marshaledConfState, err := cs.Marshal()

	if err != nil {
		return err
	}

	bucket.putMetadata([]byte(KeyConfState), marshaledConfState)

	return nil
}

func (bucket *RaftLogBucket) putMetadata(key, value []byte) {
	metadata := bucket.Bucket.Bucket([]byte(BucketRaftLogMetadata))
	metadata.Put(key, value)
}

func (bucket *RaftLogBucket) Entries() *RaftStoreEntriesCursor {
	cursor := bucket.Bucket.Bucket([]byte(BucketRaftLogEntries)).Cursor()

	return &RaftStoreEntriesCursor{
		cursor: cursor,
	}
}

func Uint64ToKey(n uint64) []byte {
	k := make([]byte, 8)

	binary.BigEndian.PutUint64(k, n)

	return k
}

func KeyToUint64(k []byte) uint64 {
	return binary.BigEndian.Uint64(k)
}

type RaftStoreEntriesCursor struct {
	cursor kv.Cursor
	index  uint64
	entry  raftpb.Entry
	err    error
}

func (cursor *RaftStoreEntriesCursor) unmarshalEntry(key, value []byte) bool {
	var entry raftpb.Entry

	if err := entry.Unmarshal(value); err != nil {
		cursor.err = fmt.Errorf("Could not unmarshal value at key %v as raftpb.Entry", key)

		return false
	}

	index := KeyToUint64(key)

	if entry.Index != index {
		cursor.err = fmt.Errorf("Key index does not match index contained inside entry: index=%d, entry=%+v", index, entry)

		return false
	}

	cursor.err = nil
	cursor.entry = entry
	cursor.index = KeyToUint64(key)

	return true
}

func (cursor *RaftStoreEntriesCursor) Seek(index uint64) bool {
	key, value := cursor.cursor.Seek(Uint64ToKey(index))

	if key == nil {
		return false
	}

	return cursor.unmarshalEntry(key, value)

}

func (cursor *RaftStoreEntriesCursor) Next() bool {
	key, value := cursor.cursor.Next()

	if key == nil {
		return false
	}

	return cursor.unmarshalEntry(key, value)
}

func (cursor *RaftStoreEntriesCursor) Prev() bool {
	key, value := cursor.cursor.Prev()

	if key == nil {
		return false
	}

	return cursor.unmarshalEntry(key, value)
}

func (cursor *RaftStoreEntriesCursor) First() bool {
	key, value := cursor.cursor.First()

	if key == nil {
		return false
	}

	return cursor.unmarshalEntry(key, value)
}

func (cursor *RaftStoreEntriesCursor) Last() bool {
	key, value := cursor.cursor.Last()

	if key == nil {
		return false
	}

	return cursor.unmarshalEntry(key, value)
}

func (cursor *RaftStoreEntriesCursor) Index() uint64 {
	return cursor.index
}

func (cursor *RaftStoreEntriesCursor) Entry() raftpb.Entry {
	return cursor.entry
}

func (cursor *RaftStoreEntriesCursor) Error() error {
	return cursor.err
}
