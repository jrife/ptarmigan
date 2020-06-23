package storage

import (
	"bytes"
	"context"
	"crypto/md5"
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

	err := update.replicaStore.apply(update.index, func(transaction mvcc.Transaction) error {
		view, err := transaction.View(mvcc.RevisionNewest)

		if err != nil && err != mvcc.ErrNoRevisions {
			return fmt.Errorf("could not create view of latest revision: %s", err)
		}

		var revision mvcc.Revision
		response, revision, err = update.executeTxnOp(logger, transaction, view, nil, txn)

		if revision != nil {
			logger.Debug("new revision created", zap.Int64("revision", revision.Revision()))
		}

		return err
	})

	logger.Debug("return", zap.Any("response", response), zap.Error(err))

	return response, err
}

func (update *update) executeTxnOp(logger *zap.Logger, transaction mvcc.Transaction, view mvcc.View, revision mvcc.Revision, r ptarmiganpb.KVTxnRequest) (ptarmiganpb.KVTxnResponse, mvcc.Revision, error) {
	var response ptarmiganpb.KVTxnResponse

	success := true

	for i, compare := range r.Compare {
		ok, err := checkCondition(logger, view, compare)

		logger.Debug("compared condition", zap.Int("i", i), zap.Bool("ok", ok), zap.Error(err))

		if err != nil {
			return ptarmiganpb.KVTxnResponse{}, nil, fmt.Errorf("could not check condition %d: %s", i, err)
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
	responseOps, revision, err := update.executeOps(logger, transaction, view, revision, ops)

	if err != nil {
		return ptarmiganpb.KVTxnResponse{}, nil, wrapError("could not execute ops", err)
	}

	response.Responses = responseOps

	return response, revision, nil
}

func checkCondition(logger *zap.Logger, view mvcc.View, compare *ptarmiganpb.Compare) (bool, error) {
	if compare == nil || view == nil {
		return true, nil
	}

	iter, err := kvMapReader(view).Keys(selectionRange(compare.Selection), kv.SortOrderAsc)

	if err != nil {
		return false, fmt.Errorf("could not create keys iterator: %s", err)
	}

	selection := stream.Pipeline(kv_marshaled.Stream(iter), selection(compare.Selection))

	for selection.Next() {
		key := selection.Value().(kv_marshaled.KV).Key()
		kv := selection.Value().(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
		kv.Key = key

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
	if predicate == nil {
		return true
	}

	switch predicate.Target {
	case ptarmiganpb.KVPredicate_CREATE:
		if _, ok := predicate.TargetUnion.(*ptarmiganpb.KVPredicate_CreateRevision); !ok {
			return false
		}

		return checkIntPredicate(kv.CreateRevision, predicate.GetCreateRevision(), predicate.Comparison)
	case ptarmiganpb.KVPredicate_VERSION:
		if _, ok := predicate.TargetUnion.(*ptarmiganpb.KVPredicate_Version); !ok {
			return false
		}

		return checkIntPredicate(kv.Version, predicate.GetVersion(), predicate.Comparison)
	case ptarmiganpb.KVPredicate_MOD:
		if _, ok := predicate.TargetUnion.(*ptarmiganpb.KVPredicate_ModRevision); !ok {
			return false
		}

		return checkIntPredicate(kv.ModRevision, predicate.GetModRevision(), predicate.Comparison)
	case ptarmiganpb.KVPredicate_LEASE:
		if _, ok := predicate.TargetUnion.(*ptarmiganpb.KVPredicate_Lease); !ok {
			return false
		}

		return checkIntPredicate(kv.Lease, predicate.GetLease(), predicate.Comparison)
	case ptarmiganpb.KVPredicate_VALUE:
		if _, ok := predicate.TargetUnion.(*ptarmiganpb.KVPredicate_Value); !ok {
			return false
		}

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

func (update *update) executeOps(logger *zap.Logger, transaction mvcc.Transaction, view mvcc.View, revision mvcc.Revision, ops []*ptarmiganpb.KVRequestOp) ([]*ptarmiganpb.KVResponseOp, mvcc.Revision, error) {
	responses := make([]*ptarmiganpb.KVResponseOp, len(ops))

	for i, op := range ops {
		var responseOp ptarmiganpb.KVResponseOp
		var err error

		logger.Debug("next op", zap.Int("i", i), zap.Any("op", op))

		switch op.Request.(type) {
		case *ptarmiganpb.KVRequestOp_RequestDelete:
			if revision == nil {
				revision, err = transaction.NewRevision()

				if err != nil {
					return nil, nil, fmt.Errorf("could not create new revision: %s", err)
				}

				view = revision
				logger = logger.With(zap.Int64("revision", revision.Revision()))
				logger.Debug("new revision")
			}

			r, err := executeDeleteOp(logger, revision, *op.GetRequestDelete())

			if err != nil {
				return nil, nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponseDelete{
				ResponseDelete: &r,
			}
		case *ptarmiganpb.KVRequestOp_RequestPut:
			if revision == nil {
				revision, err = transaction.NewRevision()

				if err != nil {
					return nil, nil, fmt.Errorf("could not create new revision: %s", err)
				}

				view = revision
				logger = logger.With(zap.Int64("revision", revision.Revision()))
				logger.Debug("new revision")
			}

			putRequest := *op.GetRequestPut()

			if !putRequest.IgnoreLease && putRequest.Lease != 0 {
				// If it doesn't exist getLease returns ErrNoSuchLease
				_, err := update.replicaStore.getLease(logger, transaction, putRequest.Lease)

				if err != nil {
					return nil, nil, err
				}
			}

			r, err := executePutOp(logger, revision, putRequest)

			if err != nil {
				return nil, nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponsePut{
				ResponsePut: &r,
			}
		case *ptarmiganpb.KVRequestOp_RequestQuery:
			var r ptarmiganpb.KVQueryResponse = ptarmiganpb.KVQueryResponse{Kvs: []*ptarmiganpb.KeyValue{}}
			var queryView mvcc.View
			var err error

			if view != nil {
				if op.GetRequestQuery().Revision == mvcc.RevisionNewest {
					queryView = view
				} else {
					queryView, err = transaction.View(op.GetRequestQuery().Revision)
				}

				switch err {
				case mvcc.ErrRevisionTooHigh:
					logger.Debug("revision number too high")
				case mvcc.ErrCompacted:
					logger.Debug("revision compacted")
				case mvcc.ErrNoRevisions:
					logger.Debug("no revisions")
				case nil:
					// view can be nil if this store has no revisions
					r, err = query(logger, queryView, *op.GetRequestQuery())

					if err != nil {
						return nil, nil, fmt.Errorf("could not execute op %d: %s", i, err)
					}
				default:
					return nil, nil, fmt.Errorf("could not retrieve revision %d: %s", op.GetRequestQuery().Revision, err)
				}
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponseQuery{
				ResponseQuery: &r,
			}
		case *ptarmiganpb.KVRequestOp_RequestTxn:
			r, rev, err := update.executeTxnOp(logger, transaction, view, revision, *op.GetRequestTxn())

			if err != nil {
				return nil, nil, fmt.Errorf("could not execute op %d: %s", i, err)
			}

			if rev != nil && revision == nil {
				revision = rev
				view = revision
				logger = logger.With(zap.Int64("revision", revision.Revision()))
			}

			responseOp.Response = &ptarmiganpb.KVResponseOp_ResponseTxn{
				ResponseTxn: &r,
			}
		default:
			logger.Debug("unrecognized transaction op type", zap.Int("i", i))
		}

		responses[i] = &responseOp
	}

	return responses, revision, nil
}

func executeDeleteOp(logger *zap.Logger, revision mvcc.Revision, r ptarmiganpb.KVDeleteRequest) (ptarmiganpb.KVDeleteResponse, error) {
	var response ptarmiganpb.KVDeleteResponse

	if r.PrevKv {
		response.PrevKvs = []*ptarmiganpb.KeyValue{}
	}

	iter, err := kvMapReader(revision).Keys(selectionRange(r.Selection), kv.SortOrderAsc)

	if err != nil {
		return ptarmiganpb.KVDeleteResponse{}, fmt.Errorf("could not create keys iterator: %s", err)
	}

	selection := stream.Pipeline(kv_marshaled.Stream(iter), selection(r.Selection))

	for selection.Next() {
		key := selection.Value().(kv_marshaled.KV).Key()
		kv := selection.Value().(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
		response.Deleted++

		if r.PrevKv {
			kv.Key = copy(key)
			response.PrevKvs = append(response.PrevKvs, &kv)
		}

		logger.Debug("delete KV", zap.Binary("key", key), zap.Any("old", kv))

		fmt.Printf("%d: Delete %x %#v\n", revision.Revision(), md5.Sum(key), key)

		if err := revision.Delete(key); err != nil {
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

	if r.PrevKv {
		response.PrevKvs = []*ptarmiganpb.KeyValue{}
	}

	if len(r.Key) != 0 {
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
			kv.Key = r.Key

			if r.PrevKv {
				response.PrevKvs = append(response.PrevKvs, &kv)
			}
		} else {
			kv.CreateRevision = revision.Revision()
		}

		newKV := updateKV(kv, r, revision.Revision())

		logger.Debug("new KV", zap.Any("value", newKV))

		fmt.Printf("%d: Put %x %#v\n", revision.Revision(), md5.Sum(r.Key), r.Key)
		if err := kvMap(revision).Put(r.Key, &newKV); err != nil {
			return ptarmiganpb.KVPutResponse{}, fmt.Errorf("could not update key %#v: %s", kv.Key, err)
		}

		return response, nil
	}

	iter, err := kvMapReader(revision).Keys(selectionRange(r.Selection), kv.SortOrderAsc)

	if err != nil {
		return ptarmiganpb.KVPutResponse{}, fmt.Errorf("could not create keys iterator: %s", err)
	}

	selection := stream.Pipeline(kv_marshaled.Stream(iter), selection(r.Selection))

	for selection.Next() {
		key := selection.Value().(kv_marshaled.KV).Key()
		kv := selection.Value().(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
		newKV := updateKV(kv, r, revision.Revision())

		if r.PrevKv {
			kv.Key = copy(key)
			response.PrevKvs = append(response.PrevKvs, &kv)
		}

		logger.Debug("update KV", zap.Binary("key", key), zap.Any("new", newKV), zap.Any("old", kv))
		fmt.Printf("%d: Put %x %#v\n", revision.Revision(), md5.Sum(key), key)

		if err := kvMap(revision).Put(key, &newKV); err != nil {
			return ptarmiganpb.KVPutResponse{}, fmt.Errorf("could not update key %#v: %s", key, err)
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
