package storage

// import (
// 	"io"
// 	"strings"
// 	"sync"

// 	"github.com/emirpasic/gods/maps/treemap"
// 	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
// 	"github.com/jrife/flock/storage/mvcc"
// )

// var _ Store = (*FakeStore)(nil)

// type FakeStore struct {
// 	replicaStores struct {
// 		sync.Mutex
// 		*treemap.Map
// 	}
// }

// func NewFakeStore() Store {
// 	fakeStore := &FakeStore{}

// 	fakeStore.replicaStores.Map = treemap.NewWith(func(a, b interface{}) int {
// 		return strings.Compare(a.(string), b.(string))
// 	})

// 	return fakeStore
// }

// func (store *FakeStore) ReplicaStores(start string, limit int) ([]string, error) {
// 	store.replicaStores.Lock()
// 	defer store.replicaStores.Unlock()

// 	replicaStores := make([]string, 0)
// 	iter := store.replicaStores.Iterator()

// 	// Seek
// 	hasMore := false
// 	for hasMore = iter.Next(); hasMore && iter.Key().(string) <= start; hasMore = iter.Next() {
// 	}

// 	for ; hasMore && len(replicaStores) < limit; hasMore = iter.Next() {
// 		replicaStores = append(replicaStores, iter.Key().(string))
// 	}

// 	return replicaStores, nil
// }

// func (store *FakeStore) ReplicaStore(name string) ReplicaStore {
// 	store.replicaStores.Lock()
// 	defer store.replicaStores.Unlock()

// 	replicaStore, ok := store.replicaStores.Get(name)

// 	if !ok {
// 		replicaStore = &FakeReplicaStore{
// 			store: store,
// 			name:  name,
// 		}

// 		store.replicaStores.Put(name, replicaStore)
// 	}

// 	return replicaStore.(*FakeReplicaStore)
// }

// func (store *FakeStore) Close() error {
// 	return nil
// }

// func (store *FakeStore) Purge() error {
// 	return nil
// }

// var _ ReplicaStore = (*FakeReplicaStore)(nil)

// type FakeReplicaStore struct {
// 	store     *FakeStore
// 	mvccStore mvcc.Store
// 	name      string
// 	metadata  []byte
// 	index     uint64
// }

// func (replicaStore *FakeReplicaStore) Name() string {
// 	return replicaStore.name
// }

// func (replicaStore *FakeReplicaStore) Create(metadata []byte) error {
// 	return nil
// }

// func (replicaStore *FakeReplicaStore) Delete() error {
// 	replicaStore.store.replicaStores.Lock()
// 	defer replicaStore.store.replicaStores.Unlock()

// 	replicaStore.store.replicaStores.Remove(replicaStore.name)

// 	return nil
// }

// func (replicaStore *FakeReplicaStore) Metadata() ([]byte, error) {
// 	return replicaStore.metadata, nil
// }

// func (replicaStore *FakeReplicaStore) Index() (uint64, error) {
// 	return replicaStore.index, nil
// }

// func (replicaStore *FakeReplicaStore) Apply(index uint64) Update {
// 	return &FakeUpdate{
// 		replicaStore: replicaStore,
// 		index:        index,
// 	}
// }

// func (replicaStore *FakeReplicaStore) Query(query ptarmiganpb.KVQueryRequest) (ptarmiganpb.KVQueryResponse, error) {
// 	return ptarmiganpb.KVQueryResponse{}, nil
// }

// func (replicaStore *FakeReplicaStore) Changes(watch ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error) {
// 	return nil, nil
// }

// func (replicaStore *FakeReplicaStore) Leases() ([]ptarmiganpb.Lease, error) {
// 	return nil, nil
// }

// func (replicaStore *FakeReplicaStore) GetLease(id int64) (ptarmiganpb.Lease, error) {
// 	return ptarmiganpb.Lease{}, nil
// }

// func (replicaStore *FakeReplicaStore) Snapshot() (io.ReadCloser, error) {
// 	return nil, nil
// }

// func (replicaStore *FakeReplicaStore) ApplySnapshot(snap io.Reader) error {
// 	return nil
// }

// type FakeUpdate struct {
// 	replicaStore *FakeReplicaStore
// 	index        uint64
// }

// func (update *FakeUpdate) Txn(txn ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error) {
// 	return ptarmiganpb.KVTxnResponse{}, nil
// }

// func (update *FakeUpdate) Compact(revision int64) error {
// 	return nil
// }

// func (update *FakeUpdate) CreateLease(ttl int64) (ptarmiganpb.Lease, error) {
// 	return ptarmiganpb.Lease{}, nil
// }

// func (update *FakeUpdate) RevokeLease(id int64) error {
// 	return nil
// }
