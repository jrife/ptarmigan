package storage_test

import (
	"github.com/emirpasic/gods/maps/treemap"
)

type ReplicaStore struct {
	index     uint64
	leases    *treemap.Map
	revisions []Revision
}

func (replicaStore *ReplicaStore) Index() uint64 {
	return replicaStore.index
}

type Revision struct {
	revision int64
	keys     *treemap.Map
	changes  *treemap.Map
}
