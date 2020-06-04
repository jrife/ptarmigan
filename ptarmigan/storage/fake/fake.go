package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
)

func compareBytes(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

func compareInt64s(a, b interface{}) int {
	if a.(int64) < b.(int64) {
		return -1
	} else if a.(int64) > b.(int64) {
		return 1
	}

	return 0
}

var _ storage.Store = (*FakeStore)(nil)

type FakeStore struct {
	replicaStores struct {
		sync.Mutex
		*treemap.Map
	}
}

func New() storage.Store {
	fakeStore := &FakeStore{}

	fakeStore.replicaStores.Map = treemap.NewWith(func(a, b interface{}) int {
		return strings.Compare(a.(string), b.(string))
	})

	return fakeStore
}

func (store *FakeStore) ReplicaStores(ctx context.Context, start string, limit int) ([]string, error) {
	store.replicaStores.Lock()
	defer store.replicaStores.Unlock()

	replicaStores := make([]string, 0)
	iter := store.replicaStores.Iterator()

	// Seek
	hasMore := false
	for hasMore = iter.Next(); hasMore && iter.Key().(string) <= start; hasMore = iter.Next() {
	}

	for ; hasMore && len(replicaStores) < limit; hasMore = iter.Next() {
		replicaStores = append(replicaStores, iter.Key().(string))
	}

	return replicaStores, nil
}

func (store *FakeStore) ReplicaStore(name string) storage.ReplicaStore {
	return &FakeReplicaStore{
		store: store,
		name:  name,
	}
}

func (store *FakeStore) Close() error {
	return nil
}

func (store *FakeStore) Purge() error {
	return nil
}

var _ storage.ReplicaStore = (*FakeReplicaStore)(nil)

type FakeReplicaStore struct {
	store *FakeStore
	name  string
}

type fakeReplicaStoreState struct {
	metadata  []byte
	revisions []fakeReplicaStoreRevision
	leases    *treemap.Map
	index     uint64
}

type fakeReplicaStoreRevision struct {
	revision int64
	keys     *treemap.Map
	changes  *treemap.Map
}

func (revision fakeReplicaStoreRevision) next() fakeReplicaStoreRevision {
	cp := fakeReplicaStoreRevision{}
	cp.revision = revision.revision + 1
	cp.keys = treemap.NewWith(compareBytes)
	cp.changes = treemap.NewWith(compareBytes)

	if revision.keys != nil {
		revision.keys.All(func(key, value interface{}) bool {
			cp.keys.Put(key, value)

			return true
		})
	}

	return cp
}

func (revision fakeReplicaStoreRevision) selection(selection *ptarmiganpb.KVSelection) []ptarmiganpb.KeyValue {
	kvs := []ptarmiganpb.KeyValue{}

	revision.keys.All(func(key, value interface{}) bool {
		if selectionMatchesKV(selection, value.(ptarmiganpb.KeyValue)) {
			kvs = append(kvs, value.(ptarmiganpb.KeyValue))
		}

		return true
	})

	return kvs
}

func selectionMatchesKV(selection *ptarmiganpb.KVSelection, kv ptarmiganpb.KeyValue) bool {
	return false
}

func (replicaStore *FakeReplicaStore) state() *fakeReplicaStoreState {
	replicaStore.store.replicaStores.Lock()
	defer replicaStore.store.replicaStores.Lock()

	state, ok := replicaStore.store.replicaStores.Get(replicaStore.name)

	if !ok {
		return nil
	}

	return state.(*fakeReplicaStoreState)
}

func (replicaStore *FakeReplicaStore) update(fn func(state *fakeReplicaStoreState) error) error {
	replicaStore.store.replicaStores.Lock()
	defer replicaStore.store.replicaStores.Lock()

	state, ok := replicaStore.store.replicaStores.Get(replicaStore.name)

	if !ok {
		return fmt.Errorf("no such replica store")
	}

	return fn(state.(*fakeReplicaStoreState))
}

func (replicaStore *FakeReplicaStore) Name() string {
	return replicaStore.name
}

func (replicaStore *FakeReplicaStore) Create(ctx context.Context, metadata []byte) error {
	replicaStore.store.replicaStores.Lock()
	defer replicaStore.store.replicaStores.Lock()

	_, ok := replicaStore.store.replicaStores.Get(replicaStore.name)

	if ok {
		return nil
	}

	replicaStore.store.replicaStores.Put(replicaStore.name, &fakeReplicaStoreState{
		metadata:  metadata,
		revisions: []fakeReplicaStoreRevision{},
		leases:    treemap.NewWith(compareInt64s),
		index:     0,
	})

	return nil
}

func (replicaStore *FakeReplicaStore) Delete(ctx context.Context) error {
	replicaStore.store.replicaStores.Lock()
	defer replicaStore.store.replicaStores.Unlock()

	replicaStore.store.replicaStores.Remove(replicaStore.name)

	return nil
}

func (replicaStore *FakeReplicaStore) Metadata(ctx context.Context) ([]byte, error) {
	state := replicaStore.state()

	if state == nil {
		return nil, fmt.Errorf("no such replica store")
	}

	return state.metadata, nil
}

func (replicaStore *FakeReplicaStore) Index(ctx context.Context) (uint64, error) {
	state := replicaStore.state()

	if state == nil {
		return 0, fmt.Errorf("no such replica store")
	}

	return state.index, nil
}

func (replicaStore *FakeReplicaStore) Apply(index uint64) storage.Update {
	return &FakeUpdate{
		replicaStore: replicaStore,
		index:        index,
	}
}

func (replicaStore *FakeReplicaStore) Query(ctx context.Context, query ptarmiganpb.KVQueryRequest) (ptarmiganpb.KVQueryResponse, error) {
	return ptarmiganpb.KVQueryResponse{}, nil
}

func (replicaStore *FakeReplicaStore) Changes(ctx context.Context, watch ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error) {
	return nil, nil
}

func (replicaStore *FakeReplicaStore) Leases(ctx context.Context) ([]ptarmiganpb.Lease, error) {
	return nil, nil
}

func (replicaStore *FakeReplicaStore) GetLease(ctx context.Context, id int64) (ptarmiganpb.Lease, error) {
	return ptarmiganpb.Lease{}, nil
}

func (replicaStore *FakeReplicaStore) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return nil, nil
}

func (replicaStore *FakeReplicaStore) ApplySnapshot(ctx context.Context, snap io.Reader) error {
	return nil
}

type FakeUpdate struct {
	replicaStore *FakeReplicaStore
	index        uint64
}

func (update *FakeUpdate) Txn(ctx context.Context, txn ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error) {
	var result ptarmiganpb.KVTxnResponse

	err := update.replicaStore.update(func(state *fakeReplicaStoreState) error {
		var lastRevision fakeReplicaStoreRevision

		if len(state.revisions) != 0 {
			lastRevision = state.revisions[len(state.revisions)-1]
		}

		revision := lastRevision.next()
		result.Succeeded = true

		for _, compare := range txn.Compare {
			if compare == nil {
				continue
			}
		}

		return nil
	})

	return result, err
}

func (update *FakeUpdate) Compact(ctx context.Context, revision int64) error {
	return nil
}

func (update *FakeUpdate) CreateLease(ctx context.Context, ttl int64) (ptarmiganpb.Lease, error) {
	return ptarmiganpb.Lease{}, nil
}

func (update *FakeUpdate) RevokeLease(ctx context.Context, id int64) error {
	return nil
}
