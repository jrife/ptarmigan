package ptarmigan

import (
	"bytes"
	"fmt"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/kv"
	kv_marshaled "github.com/jrife/flock/storage/kv/marshaled"
	"github.com/jrife/flock/storage/mvcc"
	"github.com/jrife/flock/utils/stream"
)

// Txn implements ReplicaStore.Txn
func (replicaStore *replicaStore) Txn(raftStatus ptarmiganpb.RaftStatus, txn ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error) {
	var response ptarmiganpb.KVTxnResponse

	err := replicaStore.applyRaftCommand(raftStatus, kvsNs, func(revision mvcc.Revision) error {
		var err error

		response, err = executeTxnOp(revision, txn)

		return err
	})

	return response, err
}

func executeTxnOp(revision mvcc.Revision, r ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error) {
	var response ptarmiganpb.KVTxnResponse

	success := true

	for i, compare := range r.Compare {
		ok, err := checkCondition(revision, compare)

		if err != nil {
			return ptarmiganpb.KVTxnResponse{}, fmt.Errorf("could not check condition %d: %s", i, err)
		}

		if !ok {
			success = false
			break
		}
	}

	response.Succeeded = success

	var ops []*ptarmiganpb.KVRequestOp

	if success {
		ops = r.Success
	} else {
		ops = r.Failure
	}

	responseOps, err := executeOps(revision, ops)

	if err != nil {
		return ptarmiganpb.KVTxnResponse{}, fmt.Errorf("could not execute ops: %s", err)
	}

	response.Responses = responseOps

	return response, nil
}

func checkCondition(view mvcc.View, compare *ptarmiganpb.Compare) (bool, error) {
	iter, err := kvMapReader(view).Keys(selectionRange(compare.Selection), kv.SortOrderAsc)

	if err != nil {
		return false, fmt.Errorf("could not create keys iterator: %s", err)
	}

	selection := stream.Pipeline(kv_marshaled.Stream(iter), selection(compare.Selection))

	for selection.Next() {
		if !checkPredicate(selection.Value().(ptarmiganpb.KeyValue), compare.Predicate) {
			return false, nil
		}
	}

	if selection.Error() != nil {
		return false, fmt.Errorf("iteration error: %s", err)
	}

	return true, nil
}

func checkPredicate(kv ptarmiganpb.KeyValue, predicate *ptarmiganpb.KVPredicate) bool {
	switch predicate.Target {
	case ptarmiganpb.KVPredicate_CREATE:
		return checkIntPredicate(kv.CreateRevision, predicate.GetCreateRevision(), predicate.Comparison)
	case ptarmiganpb.KVPredicate_VERSION:
		return checkIntPredicate(kv.Version, predicate.GetVersion(), predicate.Comparison)
	case ptarmiganpb.KVPredicate_MOD:
		return checkIntPredicate(kv.ModRevision, predicate.GetModRevision(), predicate.Comparison)
	case ptarmiganpb.KVPredicate_LEASE:
		return checkIntPredicate(kv.Lease, predicate.GetLease(), predicate.Comparison)
	case ptarmiganpb.KVPredicate_VALUE:
		return checkIntPredicate(int64(bytes.Compare(kv.Value, predicate.GetValue())), 0, predicate.Comparison)
	}

	return false
}

func checkIntPredicate(a, b int64, comparison ptarmiganpb.KVPredicate_Comparison) bool {
	switch comparison {
	case ptarmiganpb.KVPredicate_EQUAL:
		return a == b
	case ptarmiganpb.KVPredicate_NOT_EQUAL:
		return a != b
	case ptarmiganpb.KVPredicate_GREATER:
		return a > b
	case ptarmiganpb.KVPredicate_LESS:
		return a < b
	}

	return false
}

func executeOps(revision mvcc.Revision, ops []*ptarmiganpb.KVRequestOp) ([]*ptarmiganpb.KVResponseOp, error) {
	responses := make([]*ptarmiganpb.KVResponseOp, len(ops))

	for i, op := range ops {
		switch op.Request.(type) {
		case *ptarmiganpb.KVRequestOp_RequestDelete:
			r, err := executeDeleteOp(revision, *op.GetRequestDelete())

			if err != nil {
				return nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responses[i].Response = &ptarmiganpb.KVResponseOp_ResponseDelete{
				ResponseDelete: &r,
			}
		case *ptarmiganpb.KVRequestOp_RequestPut:
			r, err := executePutOp(revision, *op.GetRequestPut())

			if err != nil {
				return nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responses[i].Response = &ptarmiganpb.KVResponseOp_ResponsePut{
				ResponsePut: &r,
			}
		case *ptarmiganpb.KVRequestOp_RequestQuery:
			r, err := executeQueryOp(revision, *op.GetRequestQuery())

			if err != nil {
				return nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responses[i].Response = &ptarmiganpb.KVResponseOp_ResponseQuery{
				ResponseQuery: &r,
			}
		case *ptarmiganpb.KVRequestOp_RequestTxn:
			r, err := executeTxnOp(revision, *op.GetRequestTxn())

			if err != nil {
				return nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responses[i].Response = &ptarmiganpb.KVResponseOp_ResponseTxn{
				ResponseTxn: &r,
			}
		default:
			return nil, fmt.Errorf("unrecognized transaction op type: %d", i)
		}
	}

	return responses, nil
}

func executeDeleteOp(revision mvcc.Revision, r ptarmiganpb.KVDeleteRequest) (ptarmiganpb.KVDeleteResponse, error) {
	var response ptarmiganpb.KVDeleteResponse

	iter, err := kvMapReader(revision).Keys(selectionRange(r.Selection), kv.SortOrderAsc)

	if err != nil {
		return ptarmiganpb.KVDeleteResponse{}, fmt.Errorf("could not create keys iterator: %s", err)
	}

	selection := stream.Pipeline(kv_marshaled.Stream(iter), selection(r.Selection))

	for selection.Next() {
		kv := selection.Value().(ptarmiganpb.KeyValue)
		response.Deleted++

		if r.PrevKv {
			response.PrevKvs = append(response.PrevKvs, &kv)
		}

		if err := revision.Delete(kv.Key); err != nil {
			return ptarmiganpb.KVDeleteResponse{}, fmt.Errorf("could not delete key %#v: %s", kv.Key, err)
		}
	}

	if selection.Error() != nil {
		return ptarmiganpb.KVDeleteResponse{}, fmt.Errorf("iteration error: %s", err)
	}

	return response, nil
}

func executePutOp(revision mvcc.Revision, r ptarmiganpb.KVPutRequest) (ptarmiganpb.KVPutResponse, error) {
	var response ptarmiganpb.KVPutResponse

	if r.Key != nil {
		// Key overrides selection. Key lets a user create key as opposed to just
		// updating existing keys
		v, err := kvMapReader(revision).Get(r.Key)

		if err != nil {
			return ptarmiganpb.KVPutResponse{}, fmt.Errorf("could not get key %#v: %s", r.Selection.Key, err)
		}

		var kv ptarmiganpb.KeyValue

		if v != nil {
			kv = v.(ptarmiganpb.KeyValue)
		} else {
			kv.CreateRevision = revision.Revision()
		}

		newKV := updateKV(kv, r, revision.Revision())

		if err := kvMap(revision).Put(kv.Key, &newKV); err != nil {
			return ptarmiganpb.KVPutResponse{}, fmt.Errorf("could not update key %#v: %s", kv.Key, err)
		}

		return ptarmiganpb.KVPutResponse{}, nil
	}

	iter, err := kvMapReader(revision).Keys(selectionRange(r.Selection), kv.SortOrderAsc)

	if err != nil {
		return ptarmiganpb.KVPutResponse{}, fmt.Errorf("could not create keys iterator: %s", err)
	}

	selection := stream.Pipeline(kv_marshaled.Stream(iter), selection(r.Selection))

	for selection.Next() {
		kv := selection.Value().(ptarmiganpb.KeyValue)

		if r.PrevKv {
			response.PrevKvs = append(response.PrevKvs, &kv)
		}

		newKV := updateKV(kv, r, revision.Revision())

		if err := kvMap(revision).Put(kv.Key, &newKV); err != nil {
			return ptarmiganpb.KVPutResponse{}, fmt.Errorf("could not update key %#v: %s", kv.Key, err)
		}
	}

	if selection.Error() != nil {
		return ptarmiganpb.KVPutResponse{}, fmt.Errorf("iteration error: %s", err)
	}

	return response, nil
}

func updateKV(kv ptarmiganpb.KeyValue, r ptarmiganpb.KVPutRequest, revision int64) ptarmiganpb.KeyValue {
	if !r.IgnoreValue {
		kv.Value = r.Value
	}

	if kv.Value == nil {
		kv.Value = []byte{}
	}

	if !r.IgnoreLease {
		kv.Lease = r.Lease
	}

	kv.ModRevision = revision
	kv.Version++

	return kv
}

func executeQueryOp(revision mvcc.Revision, r ptarmiganpb.KVQueryRequest) (ptarmiganpb.KVQueryResponse, error) {
	return (&view{view: revision}).Query(r)
}
