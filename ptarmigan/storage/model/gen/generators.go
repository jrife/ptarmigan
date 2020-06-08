package gen

import (
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage/model"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
	"github.com/leanovate/gopter/gen"
)

// Commands returns a generator that generates a range
// of storage commands that are valid for the replica store
func Commands(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		QueryCommand(replicaStore),
		ChangesCommand(replicaStore),
		TxnCommand(replicaStore),
		CompactCommand(replicaStore),
	)
}

// QueryCommand returns a generator that generates query commands
// that are valid for the replica store
func QueryCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return KVQueryRequest(replicaStore).Map(func(queryRequest *ptarmiganpb.KVQueryRequest) commands.Command {
		return queryCommand(*queryRequest)
	})
}

// ChangesCommand returns a generator that generates changes commands
// that are valid for the replica store
func ChangesCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens().Map(func(g []interface{}) commands.Command {
		return changesCommand{}
	})
}

// TxnCommand returns a generator that generates txn commands
// that are valid for the replica store
func TxnCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return KVTxnRequest(replicaStore).Map(func(txnRequest *ptarmiganpb.KVTxnRequest) commands.Command {
		return txnCommand(*txnRequest)
	})
}

// CompactCommand returns a generator that generates compact commands
// that are valid for the replica store
func CompactCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int64().Map(func(n int64) commands.Command {
		if replicaStore.OldestRevision() == replicaStore.NewestRevision() {
			return compactCommand(0)
		}

		return compactCommand(indexFromRange(n, replicaStore.OldestRevision(), replicaStore.NewestRevision()))
	})
}

// Compare returns a generator that generates comparisons that are
// valid for the replica store
func Compare(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int64().Map(func(n int64) *ptarmiganpb.Compare {
		var compare *ptarmiganpb.Compare

		return compare
	})
}

// KVRequestOp returns a generator that generates request ops that are
// valid for the replica store
func KVRequestOp(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		// KVQueryRequest(replicaStore),
		KVPutRequest(replicaStore),
		// KVDeleteRequest(replicaStore),
		// KVTxnRequest(replicaStore),
	).Map(func(g *gopter.GenResult) *ptarmiganpb.KVRequestOp {
		var requestOp ptarmiganpb.KVRequestOp

		switch g.Result.(type) {
		case *ptarmiganpb.KVQueryRequest:
			requestOp.Request = &ptarmiganpb.KVRequestOp_RequestQuery{
				RequestQuery: g.Result.(*ptarmiganpb.KVQueryRequest),
			}
		case *ptarmiganpb.KVPutRequest:
			requestOp.Request = &ptarmiganpb.KVRequestOp_RequestPut{
				RequestPut: g.Result.(*ptarmiganpb.KVPutRequest),
			}
		case *ptarmiganpb.KVDeleteRequest:
			requestOp.Request = &ptarmiganpb.KVRequestOp_RequestDelete{
				RequestDelete: g.Result.(*ptarmiganpb.KVDeleteRequest),
			}
		case *ptarmiganpb.KVTxnRequest:
			requestOp.Request = &ptarmiganpb.KVRequestOp_RequestTxn{
				RequestTxn: g.Result.(*ptarmiganpb.KVTxnRequest),
			}
		}

		return &requestOp
	})
}

func KVQueryRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(&ptarmiganpb.KVQueryRequest{
		Limit: -1,
	})
}

func KVPutRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(&ptarmiganpb.KVPutRequest{
		Key:   []byte("aaa"),
		Value: []byte("xxx"),
	})
}

func KVDeleteRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return nil
}

func KVTxnRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.SliceOf(Compare(replicaStore)),
		gen.SliceOf(KVRequestOp(replicaStore)),
		gen.SliceOf(KVRequestOp(replicaStore)),
	).Map(func(g []interface{}) *ptarmiganpb.KVTxnRequest {
		return &ptarmiganpb.KVTxnRequest{
			Compare: g[0].([]*ptarmiganpb.Compare),
			Success: g[1].([]*ptarmiganpb.KVRequestOp),
			Failure: g[2].([]*ptarmiganpb.KVRequestOp),
		}
	})
}

// KVSelection returns a generator that generates selections that are
// valid for the replica store. The majority of possible randomly
// generated selections wouldn't match anything. This generates selections
// in a way that will increase the chances of a match.
func KVSelection(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int64().Map(func(n int64) *ptarmiganpb.KVSelection {
		var selection *ptarmiganpb.KVSelection

		return selection
	})
}

// KVPredicate returns a generator that generates predicates that are
// valid for the replica store
func KVPredicate(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int64().Map(func(n int64) *ptarmiganpb.KVPredicate {
		var predicate *ptarmiganpb.KVPredicate

		return predicate
	})
}
