package mvcc

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
)

var _ Store = (*FakeStore)(nil)

// FakeStore is an in-memory implementation of the Store interface
type FakeStore struct {
	partitions struct {
		sync.Mutex
		*treemap.Map
	}
}

// NewFakeStore creates a new fake store
func NewFakeStore() Store {
	fakeStore := &FakeStore{}

	fakeStore.partitions.Map = treemap.NewWith(func(a, b interface{}) int {
		return bytes.Compare(a.([]byte), b.([]byte))
	})

	return fakeStore
}

// Close does nothing for the fake store
func (store *FakeStore) Close() error {
	return nil
}

// Delete does nothing for the fake store
func (store *FakeStore) Delete() error {
	return nil
}

// Partitions implements Store.Partitions
func (store *FakeStore) Partitions(names keys.Range, limit int) ([][]byte, error) {
	store.partitions.Lock()
	defer store.partitions.Unlock()

	partitions := make([][]byte, 0)
	iter := store.partitions.Iterator()

	// Seek
	hasMore := false
	for hasMore = iter.Next(); hasMore && (names.Min == nil || bytes.Compare(iter.Key().([]byte), names.Min) < 0); hasMore = iter.Next() {
	}

	for ; hasMore && len(partitions) < limit && (names.Max == nil || bytes.Compare(iter.Key().([]byte), names.Max) < 0); hasMore = iter.Next() {
		partitions = append(partitions, iter.Key().([]byte))
	}

	return partitions, nil
}

// Partition implements Store.Partition
func (store *FakeStore) Partition(name []byte) Partition {
	store.partitions.Lock()
	defer store.partitions.Unlock()

	replicaStore, ok := store.partitions.Get(name)

	if !ok {
		replicaStore = &fakePartition{
			store: store,
			name:  name,
		}

		store.partitions.Put(name, replicaStore)
	}

	return replicaStore.(*fakePartition)
}

type fakePartition struct {
	store   *FakeStore
	name    []byte
	State   fakePartitionState
	writeMu sync.Mutex
}

type fakePartitionState struct {
	Metadata  []byte
	Revisions []fakeRevision
}

func (partition *fakePartition) Name() []byte {
	return partition.name
}

func (partition *fakePartition) Create(metadata []byte) error {
	if partition.State.Metadata == nil {
		partition.State.Metadata = metadata
	}

	return nil
}

func (partition *fakePartition) Delete() error {
	partition.store.partitions.Lock()
	defer partition.store.partitions.Unlock()

	partition.store.partitions.Remove(partition.name)

	return nil
}

func (partition *fakePartition) Metadata() ([]byte, error) {
	return partition.State.Metadata, nil
}

func (partition *fakePartition) Begin(writeable bool) (Transaction, error) {
	if writeable {
		partition.writeMu.Lock()
	}

	return &fakeTransaction{
		partition: partition,
		revisions: partition.State.Revisions,
		writable:  writeable,
	}, nil
}

func (partition *fakePartition) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	buffer := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(partition); err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(buffer.Bytes())), nil
}

func (partition *fakePartition) ApplySnapshot(ctx context.Context, snap io.Reader) error {
	decoder := gob.NewDecoder(snap)
	return decoder.Decode(partition)
}

type fakeTransaction struct {
	partition   *fakePartition
	newRevision *fakeRevision
	revisions   []fakeRevision
	close       sync.Once
	writable    bool
}

func (transaction *fakeTransaction) NewRevision() (Revision, error) {
	if !transaction.writable {
		return nil, fmt.Errorf("transaction is read-only")
	}

	if transaction.newRevision != nil {
		return nil, ErrTooManyRevisions
	}

	rev := &fakeRevision{
		fakeView: fakeView{
			State: fakeRevisionState{
				Keys:    make(map[string][]byte),
				Changes: make(map[string][]byte),
			},
			keys:    kv.NewFakeMap(),
			changes: kv.NewFakeMap(),
		},
	}

	if len(transaction.revisions) > 0 {
		rev.fakeView.LastState = transaction.revisions[len(transaction.revisions)-1].State
	}

	rev.fakeView.State.Revision = rev.fakeView.LastState.Revision + 1

	for key, value := range rev.fakeView.LastState.Keys {
		rev.fakeView.keys.Put([]byte(key), value)
		rev.fakeView.State.Keys[key] = value
	}

	transaction.newRevision = rev

	return rev, nil
}

func (transaction *fakeTransaction) View(revision int64) (View, error) {
	if len(transaction.revisions) == 0 {
		return nil, ErrNoRevisions
	}

	if revision == RevisionNewest {
		return &transaction.revisions[len(transaction.revisions)-1].fakeView, nil
	} else if revision == RevisionOldest {
		return &transaction.revisions[0].fakeView, nil
	}

	if revision < transaction.revisions[0].Revision() {
		return nil, ErrCompacted
	} else if revision > transaction.revisions[len(transaction.revisions)-1].Revision() {
		return nil, ErrRevisionTooHigh
	}

	for _, r := range transaction.revisions {
		if revision == r.Revision() {
			return &r.fakeView, nil
		}
	}

	return nil, fmt.Errorf("could not find revision")
}

func (transaction *fakeTransaction) Compact(revision int64) error {
	if !transaction.writable {
		return fmt.Errorf("transaction is read-only")
	}

	if len(transaction.revisions) == 0 {
		return ErrNoRevisions
	}

	if revision == RevisionNewest {
		revision = transaction.revisions[len(transaction.revisions)-1].Revision()
	} else if revision == RevisionOldest {
		return nil
	}

	if revision < transaction.revisions[0].Revision() {
		return ErrCompacted
	} else if revision > transaction.revisions[len(transaction.revisions)-1].Revision() {
		return ErrRevisionTooHigh
	}

	for i, r := range transaction.revisions {
		if revision == r.Revision() {
			transaction.revisions = transaction.revisions[i:]

			break
		}
	}

	return nil
}

func (transaction *fakeTransaction) Commit() error {
	if !transaction.writable {
		return fmt.Errorf("transaction is read-only")
	}

	if transaction.newRevision != nil {
		transaction.revisions = append(transaction.revisions, *transaction.newRevision)
	}

	transaction.partition.State.Revisions = transaction.revisions
	transaction.close.Do(transaction.partition.writeMu.Unlock)

	return nil
}

func (transaction *fakeTransaction) Rollback() error {
	transaction.close.Do(transaction.partition.writeMu.Unlock)

	return nil
}

type fakeRevision struct {
	fakeView
}

func (revision *fakeRevision) Put(key, value []byte) error {
	if len(key) == 0 || value == nil {
		return nil
	}

	revision.keys.Put(key, value)
	revision.State.Keys[string(key)] = value
	revision.changes.Put(key, value)
	revision.State.Changes[string(key)] = value

	return nil
}

func (revision *fakeRevision) Delete(key []byte) error {
	revision.keys.Delete(key)
	delete(revision.State.Keys, string(key))

	if _, ok := revision.LastState.Keys[string(key)]; ok {
		revision.changes.Put(key, nil)
		revision.State.Changes[string(key)] = nil
	} else {
		revision.changes.Delete(key)
		delete(revision.State.Changes, string(key))
	}

	return nil
}

type fakeView struct {
	LastState fakeRevisionState
	State     fakeRevisionState
	keys      kv.Map
	changes   kv.Map
}

type fakeRevisionState struct {
	Keys     map[string][]byte
	Changes  map[string][]byte
	Revision int64
}

func (view *fakeView) Get(key []byte) ([]byte, error) {
	return view.keys.Get(key)
}

func (view *fakeView) Keys(keys keys.Range, order kv.SortOrder) (kv.Iterator, error) {
	return view.keys.Keys(keys, order)
}

func (view *fakeView) Changes(keys keys.Range, includePrev bool) (DiffIterator, error) {
	iter, err := view.changes.Keys(keys, kv.SortOrderAsc)

	if err != nil {
		return nil, err
	}

	return &fakeDiffIterator{Iterator: iter, lastState: view.LastState}, nil
}

func (view *fakeView) Revision() int64 {
	return view.State.Revision
}

func (view *fakeView) GobDecode(data []byte) error {
	decoder := gob.NewDecoder(bytes.NewBuffer(data))
	if err := decoder.Decode(&view.State); err != nil {
		return err
	}

	if err := decoder.Decode(&view.LastState); err != nil {
		return err
	}

	for key, value := range view.State.Keys {
		view.keys.Put([]byte(key), value)
	}

	for key, value := range view.State.Changes {
		view.changes.Put([]byte(key), value)
	}

	return nil
}

func (view *fakeView) GobEncode() ([]byte, error) {
	buffer := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(view.State); err != nil {
		return nil, err
	}

	if err := encoder.Encode(view.LastState); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

type fakeDiffIterator struct {
	kv.Iterator
	lastState fakeRevisionState
}

func (diffIterator *fakeDiffIterator) IsPut() bool {
	return diffIterator.Value() != nil
}

func (diffIterator *fakeDiffIterator) IsDelete() bool {
	return !diffIterator.IsPut()
}

func (diffIterator *fakeDiffIterator) Prev() []byte {
	return diffIterator.lastState.Keys[string(diffIterator.Key())]
}
