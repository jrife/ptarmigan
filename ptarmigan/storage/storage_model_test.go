package storage_test

import (
	"bytes"
	"context"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
)

func compareBytes(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

type ReplicaStore struct {
	index      uint64
	leases     *treemap.Map
	revisions  []Revision
	lastReturn interface{}
	lastError  error
}

func NewReplicaStore() *ReplicaStore {
	return &ReplicaStore{
		revisions: []Revision{},
		leases:    treemap.NewWith(compareBytes),
	}
}

func (replicaStore *ReplicaStore) Index() uint64 {
	return replicaStore.index
}

func (replicaStore *ReplicaStore) Apply(index uint64) storage.Update {

}

type Update struct {
}

func (update *Update) Txn(ctx context.Context, txn ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error) {
	return ptarmiganpb.KVTxnResponse{}, nil
}

func (update *Update) Compact(ctx context.Context, revision int64) error {
	return nil
}

func (update *Update) CreateLease(ctx context.Context, ttl int64) (ptarmiganpb.Lease, error)
func (update *Update) RevokeLease(ctx context.Context, id int64) error

type Revision struct {
	revision int64
	keys     *treemap.Map
	changes  *treemap.Map
}
