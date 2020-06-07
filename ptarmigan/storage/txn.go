package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/kv"
	kv_marshaled "github.com/jrife/flock/storage/kv/marshaled"
	"github.com/jrife/flock/storage/mvcc"
	"github.com/jrife/flock/utils/log"
	"github.com/jrife/flock/utils/stream"
	"go.uber.org/zap"
)

// Txn implements Update.Txn
func (update *update) Txn(ctx context.Context, txn ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error) {
	logger := log.WithContext(ctx, update.logger).With(zap.String("operation", "Txn"))
	logger.Debug("start", zap.Any("txn", txn))

	var response ptarmiganpb.KVTxnResponse

	err := update.replicaStore.applyUpdate(update.index, kvsNs, func(revision mvcc.Revision) error {
		var err error

		response, err = executeTxnOp(logger.With(zap.Int64("revision", revision.Revision())), revision, txn)

		return err
	})

	logger.Debug("return", zap.Any("response", response), zap.Error(err))

	return response, err
}

func executeTxnOp(logger *zap.Logger, revision mvcc.Revision, r ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, error) {
	var response ptarmiganpb.KVTxnResponse

	success := true

	for i, compare := range r.Compare {
		ok, err := checkCondition(logger, revision, compare)

		logger.Debug("compared condition", zap.Int("i", i), zap.Bool("ok", ok), zap.Error(err))

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

	logger.Debug("compare result", zap.Bool("success", success))

	if success {
		ops = r.Success
	} else {
		ops = r.Failure
	}

	logger.Debug("ops", zap.Any("ops", ops))
	responseOps, err := executeOps(logger, revision, ops)

	if err != nil {
		return ptarmiganpb.KVTxnResponse{}, fmt.Errorf("could not execute ops: %s", err)
	}

	response.Responses = responseOps

	return response, nil
}

func checkCondition(logger *zap.Logger, view mvcc.View, compare *ptarmiganpb.Compare) (bool, error) {
	iter, err := kvMapReader(view).Keys(selectionRange(compare.Selection), kv.SortOrderAsc)

	if err != nil {
		return false, fmt.Errorf("could not create keys iterator: %s", err)
	}

	selection := stream.Pipeline(kv_marshaled.Stream(iter), selection(compare.Selection))

	for selection.Next() {
		kv := selection.Value().(ptarmiganpb.KeyValue)

		logger.Debug("next KeyValue", zap.Any("value", kv))

		if !checkPredicate(kv, compare.Predicate) {
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

func executeOps(logger *zap.Logger, revision mvcc.Revision, ops []*ptarmiganpb.KVRequestOp) ([]*ptarmiganpb.KVResponseOp, error) {
	responses := make([]*ptarmiganpb.KVResponseOp, len(ops))

	for i, op := range ops {
		var responseOp ptarmiganpb.KVResponseOp

		logger.Debug("next op", zap.Int("i", i), zap.Any("op", op))

		switch op.Request.(type) {
		case *ptarmiganpb.KVRequestOp_RequestDelete:
			r, err := executeDeleteOp(logger, revision, *op.GetRequestDelete())

			if err != nil {
				return nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponseDelete{
				ResponseDelete: &r,
			}
		case *ptarmiganpb.KVRequestOp_RequestPut:
			r, err := executePutOp(logger, revision, *op.GetRequestPut())

			if err != nil {
				return nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponsePut{
				ResponsePut: &r,
			}
		case *ptarmiganpb.KVRequestOp_RequestQuery:
			r, err := query(logger, revision, *op.GetRequestQuery())

			if err != nil {
				return nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponseQuery{
				ResponseQuery: &r,
			}
		case *ptarmiganpb.KVRequestOp_RequestTxn:
			r, err := executeTxnOp(logger, revision, *op.GetRequestTxn())

			if err != nil {
				return nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponseTxn{
				ResponseTxn: &r,
			}
		default:
			return nil, fmt.Errorf("unrecognized transaction op type: %d", i)
		}

		responses[i] = &responseOp
	}

	return responses, nil
}

func executeDeleteOp(logger *zap.Logger, revision mvcc.Revision, r ptarmiganpb.KVDeleteRequest) (ptarmiganpb.KVDeleteResponse, error) {
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

func executePutOp(logger *zap.Logger, revision mvcc.Revision, r ptarmiganpb.KVPutRequest) (ptarmiganpb.KVPutResponse, error) {
	var response ptarmiganpb.KVPutResponse

	if r.Key != nil {
		// Key overrides selection. Key lets a user create key as opposed to just
		// updating existing keys
		logger.Debug("single key", zap.Binary("key", r.Key))

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

		logger.Debug("new KV", zap.Any("value", newKV))

		if err := kvMap(revision).Put(r.Key, &newKV); err != nil {
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