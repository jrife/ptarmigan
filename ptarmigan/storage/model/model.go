package model

import (
	"bytes"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/mvcc"
)

// ReplicaStoreModel is an in-memory model of a replica store
// It is used in property-based tests as a reference implementation
// of how a replica store should work.
type ReplicaStoreModel struct {
	revisions []RevisionModel
	leases    *treemap.Map
	index     uint64
	lease     int64
	response  interface{}
}

// NewReplicaStoreModel creates an empty replica store model
func NewReplicaStoreModel() *ReplicaStoreModel {
	return &ReplicaStoreModel{
		revisions: []RevisionModel{},
		leases:    treemap.NewWith(compareInt64s),
	}
}

func (replicaStoreModel ReplicaStoreModel) revision(r int64) (RevisionModel, int) {
	if len(replicaStoreModel.revisions) == 0 {
		return RevisionModel{}, -1
	}

	if r == mvcc.RevisionNewest {
		r = replicaStoreModel.revisions[len(replicaStoreModel.revisions)-1].revision
	} else if r <= mvcc.RevisionOldest {
		r = replicaStoreModel.revisions[0].revision
	}

	for i, revision := range replicaStoreModel.revisions {
		if revision.revision == r {
			return revision, i
		}
	}

	return RevisionModel{}, -1
}

// DeepCopy produces a deep copy of this replica store model
func (replicaStoreModel *ReplicaStoreModel) DeepCopy() *ReplicaStoreModel {
	deepCopy := NewReplicaStoreModel()

	deepCopy.index = replicaStoreModel.index
	deepCopy.lease = replicaStoreModel.lease
	deepCopy.response = replicaStoreModel.response

	for _, revision := range replicaStoreModel.revisions {
		deepCopy.revisions = append(deepCopy.revisions, revision.DeepCopy())
	}

	replicaStoreModel.leases.Each(func(key, value interface{}) {
		deepCopy.leases.Put(key, value)
	})

	return deepCopy
}

// LastResponse returns the return value of the last command processed by
// the model
func (replicaStoreModel *ReplicaStoreModel) LastResponse() interface{} {
	return replicaStoreModel.response
}

// ApplyTxn executes an  ApplyTxn command on the model
func (replicaStoreModel *ReplicaStoreModel) ApplyTxn(index uint64, txn ptarmiganpb.KVTxnRequest) ptarmiganpb.KVTxnResponse {
	if replicaStoreModel.index >= index {
		replicaStoreModel.response = ptarmiganpb.KVTxnResponse{}
		return ptarmiganpb.KVTxnResponse{}
	}

	replicaStoreModel.index = index
	var lastRevision RevisionModel

	if len(replicaStoreModel.revisions) != 0 {
		lastRevision = replicaStoreModel.revisions[len(replicaStoreModel.revisions)-1]
	} else {
		lastRevision.changes = treemap.NewWith(compareBytes)
		lastRevision.kvs = treemap.NewWith(compareBytes)
	}

	revision := lastRevision.Next()
	result, commit := replicaStoreModel.txn(txn, lastRevision, revision)

	if !commit {
		replicaStoreModel.response = result
		return result
	}

	replicaStoreModel.revisions = append(replicaStoreModel.revisions, revision)
	replicaStoreModel.response = result

	return result
}

func (replicaStoreModel *ReplicaStoreModel) txn(txn ptarmiganpb.KVTxnRequest, lastRevision RevisionModel, revision RevisionModel) (ptarmiganpb.KVTxnResponse, bool) {
	var result ptarmiganpb.KVTxnResponse
	var commit bool

	result.Succeeded = true
	result.Responses = []*ptarmiganpb.KVResponseOp{}
	kvs := revision.AllKvs()

	for _, compare := range txn.Compare {
		if compare == nil {
			continue
		}

		selection := kvs.selection(compare.Selection)
		// are there any kvs in the selection that don't match the predicate?
		if len(selection) != len(selection.filter(compare.Predicate)) {
			result.Succeeded = false

			break
		}
	}

	var ops []*ptarmiganpb.KVRequestOp

	if result.Succeeded {
		ops = txn.Success
	} else {
		ops = txn.Failure
	}

	for _, op := range ops {
		var responseOp ptarmiganpb.KVResponseOp

		switch op.Request.(type) {
		case *ptarmiganpb.KVRequestOp_RequestDelete:
			commit = true

			var resp ptarmiganpb.KVDeleteResponse

			if op.GetRequestDelete().PrevKv {
				resp.PrevKvs = []*ptarmiganpb.KeyValue{}
			}

			kvs := revision.AllKvs().selection(op.GetRequestDelete().Selection)

			for i := range kvs {
				if lastKVRaw, found := lastRevision.kvs.Get(kvs[i].Key); found {
					// This key existed before this transaction. Need a DELETE event
					lastKV := lastKVRaw.(ptarmiganpb.KeyValue)
					revision.changes.Put(kvs[i].Key, ptarmiganpb.Event{Type: ptarmiganpb.Event_DELETE, Kv: &ptarmiganpb.KeyValue{Key: kvs[i].Key, ModRevision: revision.revision}, PrevKv: &lastKV})
				} else {
					// This key was entirely created within this transaction. No need for a DELETE event
					revision.changes.Remove(kvs[i].Key)
				}

				revision.kvs.Remove(kvs[i].Key)
				resp.Deleted++

				if op.GetRequestDelete().PrevKv {
					resp.PrevKvs = append(resp.PrevKvs, &kvs[i])
				}
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponseDelete{
				ResponseDelete: &resp,
			}
		case *ptarmiganpb.KVRequestOp_RequestPut:
			commit = true

			if _, ok := replicaStoreModel.leases.Get(op.GetRequestPut().Lease); !op.GetRequestPut().IgnoreLease && op.GetRequestPut().Lease != 0 && !ok {
				return ptarmiganpb.KVTxnResponse{}, false
			}

			var resp ptarmiganpb.KVPutResponse
			var kvs []ptarmiganpb.KeyValue

			if op.GetRequestPut().PrevKv {
				resp.PrevKvs = []*ptarmiganpb.KeyValue{}
			}

			if len(op.GetRequestPut().Key) != 0 {
				rawKV, ok := revision.kvs.Get(op.GetRequestPut().Key)

				if ok {
					kvs = append(kvs, rawKV.(ptarmiganpb.KeyValue))

					if op.GetRequestPut().PrevKv {
						resp.PrevKvs = []*ptarmiganpb.KeyValue{&kvs[0]}
					}
				} else {
					kvs = append(kvs, ptarmiganpb.KeyValue{
						// Weirdness with a byte flipping in the middle of this key
						// maybe somewhere in the bolt driver? Copy the key here
						// to prevent this.
						Key:            append([]byte{}, op.GetRequestPut().Key...),
						CreateRevision: revision.revision,
					})
				}
			} else {
				kvs = revision.AllKvs().selection(op.GetRequestPut().Selection)

				if op.GetRequestPut().PrevKv {
					for i := range kvs {
						resp.PrevKvs = append(resp.PrevKvs, &kvs[i])
					}
				}
			}

			for i := range kvs {
				var newKV = kvs[i]

				if !op.GetRequestPut().IgnoreLease {
					newKV.Lease = op.GetRequestPut().Lease
				}

				if !op.GetRequestPut().IgnoreValue {
					newKV.Value = op.GetRequestPut().Value
				}

				if newKV.Value == nil {
					newKV.Value = []byte{}
				}

				newKV.ModRevision = revision.revision
				newKV.Version++

				if lastKVRaw, found := lastRevision.kvs.Get(newKV.Key); found {
					lastKV := lastKVRaw.(ptarmiganpb.KeyValue)
					revision.changes.Put(newKV.Key, ptarmiganpb.Event{Type: ptarmiganpb.Event_PUT, Kv: &newKV, PrevKv: &lastKV})
				} else {
					revision.changes.Put(newKV.Key, ptarmiganpb.Event{Type: ptarmiganpb.Event_PUT, Kv: &newKV, PrevKv: nil})
				}

				revision.kvs.Put(newKV.Key, newKV)
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponsePut{
				ResponsePut: &resp,
			}
		case *ptarmiganpb.KVRequestOp_RequestQuery:
			// Query should see the uncommitted revision.
			replicaStoreModelCopy := *replicaStoreModel
			replicaStoreModelCopy.revisions = append([]RevisionModel{}, replicaStoreModelCopy.revisions...)
			replicaStoreModelCopy.revisions = append(replicaStoreModelCopy.revisions, revision)

			resp, ok := query(*op.GetRequestQuery(), &replicaStoreModelCopy)

			if !ok {
				resp.Kvs = []*ptarmiganpb.KeyValue{}
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponseQuery{
				ResponseQuery: &resp,
			}
		case *ptarmiganpb.KVRequestOp_RequestTxn:
			resp, c := replicaStoreModel.txn(*op.GetRequestTxn(), lastRevision, revision)

			if c {
				commit = true
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponseTxn{
				ResponseTxn: &resp,
			}
		}

		result.Responses = append(result.Responses, &responseOp)
	}

	return result, commit
}

// Index executes an Index command on the model
func (replicaStoreModel *ReplicaStoreModel) Index() uint64 {
	return replicaStoreModel.index
}

// ApplyCompact executes an ApplyCompact command on the model
func (replicaStoreModel *ReplicaStoreModel) ApplyCompact(index uint64, revision int64) {
	if replicaStoreModel.index >= index {
		return
	}

	replicaStoreModel.index = index

	if len(replicaStoreModel.revisions) == 0 {
		return
	}

	if revision == mvcc.RevisionNewest {
		revision = replicaStoreModel.revisions[len(replicaStoreModel.revisions)-1].revision
	} else if revision == mvcc.RevisionOldest {
		revision = replicaStoreModel.revisions[0].revision
	}

	for i, r := range replicaStoreModel.revisions {
		if r.revision == revision {
			replicaStoreModel.revisions = replicaStoreModel.revisions[i:]

			for _, change := range replicaStoreModel.revisions[0].AllChanges() {
				change.PrevKv = nil
				replicaStoreModel.revisions[0].changes.Put(change.Kv.Key, change)
			}

			return
		}
	}
}

// ApplyCreateLease executes an ApplyCreateLease command on the model
func (replicaStoreModel *ReplicaStoreModel) ApplyCreateLease(index uint64, ttl int64) ptarmiganpb.Lease {
	if replicaStoreModel.index >= index {
		replicaStoreModel.response = ptarmiganpb.Lease{}
		return ptarmiganpb.Lease{}
	}

	replicaStoreModel.index = index

	var lease ptarmiganpb.Lease
	replicaStoreModel.lease++
	lease.GrantedTTL = ttl
	lease.ID = replicaStoreModel.lease

	replicaStoreModel.leases.Put(lease.ID, lease)

	replicaStoreModel.response = lease
	return lease
}

// ApplyRevokeLease executes an ApplyRevokeLease command on the model
func (replicaStoreModel *ReplicaStoreModel) ApplyRevokeLease(index uint64, id int64) {
	if replicaStoreModel.index >= index {
		return
	}

	replicaStoreModel.index = index

	_, found := replicaStoreModel.leases.Get(id)

	if !found {
		return
	}

	replicaStoreModel.leases.Remove(id)
	var lastRevision RevisionModel

	if len(replicaStoreModel.revisions) != 0 {
		lastRevision = replicaStoreModel.revisions[len(replicaStoreModel.revisions)-1]
	} else {
		lastRevision.changes = treemap.NewWith(compareBytes)
		lastRevision.kvs = treemap.NewWith(compareBytes)
	}

	revision := lastRevision.Next()
	_, commit := replicaStoreModel.txn(ptarmiganpb.KVTxnRequest{
		Success: []*ptarmiganpb.KVRequestOp{
			{
				Request: &ptarmiganpb.KVRequestOp_RequestDelete{
					RequestDelete: &ptarmiganpb.KVDeleteRequest{
						Selection: &ptarmiganpb.KVSelection{
							Lease: id,
						},
					},
				},
			},
		},
	}, lastRevision, revision)

	if !commit {
		return
	}

	replicaStoreModel.revisions = append(replicaStoreModel.revisions, revision)
}

// Query executes a Query command on the model
func (replicaStoreModel *ReplicaStoreModel) Query(request ptarmiganpb.KVQueryRequest) ptarmiganpb.KVQueryResponse {
	response, _ := query(request, replicaStoreModel)
	return response
}

// Changes executes a Changes command on the model
func (replicaStoreModel *ReplicaStoreModel) Changes(watch ptarmiganpb.KVWatchRequest, limit int) []ptarmiganpb.Event {
	changes := []ptarmiganpb.Event{}

	if watch.Start == nil {
		watch.Start = &ptarmiganpb.KVWatchCursor{Revision: mvcc.RevisionOldest}
	}

	startRevision, index := replicaStoreModel.revision(watch.Start.Revision)

	if index == -1 {
		return changes
	}

	for _, change := range startRevision.AllChanges().selection(watch.Selection) {
		if watch.Start != nil && bytes.Compare(change.Kv.Key, watch.Start.Key) <= 0 {
			continue
		}

		if watch.NoDelete && change.Type == ptarmiganpb.Event_DELETE {
			continue
		}

		if watch.NoPut && change.Type == ptarmiganpb.Event_PUT {
			continue
		}

		changes = append(changes, change)
	}

	for i := index + 1; i < len(replicaStoreModel.revisions); i++ {
		for _, change := range replicaStoreModel.revisions[i].AllChanges().selection(watch.Selection) {
			if watch.NoDelete && change.Type == ptarmiganpb.Event_DELETE {
				continue
			}

			if watch.NoPut && change.Type == ptarmiganpb.Event_PUT {
				continue
			}

			changes = append(changes, change)
		}
	}

	if !watch.PrevKv {
		for i, change := range changes {
			change.PrevKv = nil
			changes[i] = change
		}
	}

	if limit > 0 && limit < len(changes) {
		changes = changes[:limit]
	}

	return changes
}

// Leases executes a Leases command on the model
func (replicaStoreModel *ReplicaStoreModel) Leases() []ptarmiganpb.Lease {
	leases := []ptarmiganpb.Lease{}

	replicaStoreModel.leases.Each(func(key, value interface{}) {
		leases = append(leases, value.(ptarmiganpb.Lease))
	})

	return leases
}

// GetLease executes a GetLease command on the model
func (replicaStoreModel *ReplicaStoreModel) GetLease(id int64) ptarmiganpb.Lease {
	lease, found := replicaStoreModel.leases.Get(id)

	if !found {
		return ptarmiganpb.Lease{}
	}

	return lease.(ptarmiganpb.Lease)
}

// NewestRevision executes a NewestRevision command on the model
func (replicaStoreModel *ReplicaStoreModel) NewestRevision() int64 {
	if len(replicaStoreModel.revisions) == 0 {
		return 0
	}

	return replicaStoreModel.revisions[len(replicaStoreModel.revisions)-1].revision
}

// OldestRevision executes a OldestRevision command on the model
func (replicaStoreModel *ReplicaStoreModel) OldestRevision() int64 {
	if len(replicaStoreModel.revisions) == 0 {
		return 0
	}

	return replicaStoreModel.revisions[0].revision
}

// RevisionModel is an in-memory model of a replica store revision
type RevisionModel struct {
	revision int64
	changes  *treemap.Map
	kvs      *treemap.Map
}

// DeepCopy produces a deep copy of this revision model
func (revisionModel RevisionModel) DeepCopy() RevisionModel {
	deepCopy := RevisionModel{revision: revisionModel.revision}

	if revisionModel.kvs != nil {
		deepCopy.kvs = treemap.NewWith(compareBytes)
		revisionModel.kvs.Each(func(key, value interface{}) {
			deepCopy.kvs.Put(key, value)
		})
	}

	if revisionModel.changes != nil {
		deepCopy.changes = treemap.NewWith(compareBytes)
		revisionModel.changes.Each(func(key, value interface{}) {
			deepCopy.changes.Put(key, value)
		})
	}

	return deepCopy
}

// Next generates a copy of this revision whose revision number is
// one more than this revision's revision number. The copy's changes
// will be empty.
func (revisionModel RevisionModel) Next() RevisionModel {
	nextRevision := RevisionModel{
		revision: revisionModel.revision + 1,
		changes:  treemap.NewWith(compareBytes),
		kvs:      treemap.NewWith(compareBytes),
	}

	if revisionModel.kvs != nil {
		revisionModel.kvs.Each(func(key, value interface{}) {
			nextRevision.kvs.Put(key, value)
		})
	}

	return nextRevision
}

// AllKvs returns a list of all the KVs in the revision
func (revisionModel RevisionModel) AllKvs() kvList {
	var kvs []ptarmiganpb.KeyValue = []ptarmiganpb.KeyValue{}

	if revisionModel.kvs != nil {
		revisionModel.kvs.Each(func(key, value interface{}) {
			kvs = append(kvs, value.(ptarmiganpb.KeyValue))
		})
	}

	return kvs
}

// AllChanges returns a list of all the changes in the revision
func (revisionModel RevisionModel) AllChanges() eventList {
	var changes []ptarmiganpb.Event = []ptarmiganpb.Event{}

	if revisionModel.changes != nil {
		revisionModel.changes.Each(func(key, value interface{}) {
			changes = append(changes, value.(ptarmiganpb.Event))
		})
	}

	return changes
}
