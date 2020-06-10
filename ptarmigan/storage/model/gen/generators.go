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
	return KVQueryRequest(replicaStore).SuchThat(func(queryRequest *ptarmiganpb.KVQueryRequest) bool {
		return queryRequest != nil
	}).Map(func(queryRequest *ptarmiganpb.KVQueryRequest) commands.Command {
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
	return KVTxnRequest(replicaStore).SuchThat(func(txnRequest *ptarmiganpb.KVTxnRequest) bool {
		return txnRequest != nil
	}).Map(func(txnRequest *ptarmiganpb.KVTxnRequest) commands.Command {
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
		KVDeleteRequest(replicaStore),
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

func Revision(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int64()
}

func ExistingRevision(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return Revision(replicaStore).Map(func(n int64) int64 {
		if replicaStore.OldestRevision() == replicaStore.NewestRevision() {
			return n
		}

		return indexFromRange(n, replicaStore.OldestRevision(), replicaStore.NewestRevision())
	})
}

func ExistingLease(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int().Map(func(i int) int64 {
		leases := replicaStore.Leases()

		if len(leases) == 0 {
			return 0
		}

		return leases[i%len(leases)].ID
	})
}

func Value(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.AnyString().Map(func(str string) []byte {
		return []byte(str)
	})
}

func Key(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.AnyString().Map(func(str string) []byte {
		return []byte(str)
	})
}

func ExistingKey(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int().Map(func(i int) []byte {
		kvs := replicaStore.Query(ptarmiganpb.KVQueryRequest{}).Kvs

		if len(kvs) == 0 {
			return nil
		}

		return kvs[i%len(kvs)].Key
	})
}

func KVQueryRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gen.Const(ptarmiganpb.KVQueryRequest{
		Limit: -1,
	}))
}

func KVPutRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gopter.CombineGens(
		KVSelection(replicaStore),
		gen.PtrOf(Key(replicaStore)),
		gen.PtrOf(Value(replicaStore)),
		ExistingLease(replicaStore),
		gen.Bool(),
		gen.Bool(),
		gen.Bool(),
	).Map(func(g []interface{}) ptarmiganpb.KVPutRequest {
		var putRequest ptarmiganpb.KVPutRequest

		putRequest.Selection = g[0].(*ptarmiganpb.KVSelection)

		if g[1] != nil {
			putRequest.Key = *g[1].(*[]byte)
		}

		if g[2] != nil {
			putRequest.Value = *g[2].(*[]byte)
		}

		putRequest.Lease = g[3].(int64)
		//putRequest.PrevKv = g[4].(bool)
		putRequest.IgnoreLease = g[5].(bool)
		putRequest.IgnoreValue = g[6].(bool)

		return putRequest
	}))
}

func KVDeleteRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gopter.CombineGens(
		KVSelection(replicaStore),
		gen.Bool(),
	).Map(func(g []interface{}) ptarmiganpb.KVDeleteRequest {
		var deleteRequest ptarmiganpb.KVDeleteRequest

		deleteRequest.Selection = g[0].(*ptarmiganpb.KVSelection)
		//deleteRequest.PrevKv = g[1].(bool)

		return deleteRequest
	}))
}

func KVTxnRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gopter.CombineGens(
		gen.PtrOf(gen.SliceOf(Compare(replicaStore))),
		gen.PtrOf(gen.SliceOf(KVRequestOp(replicaStore))),
		gen.PtrOf(gen.SliceOf(KVRequestOp(replicaStore))),
	).Map(func(g []interface{}) ptarmiganpb.KVTxnRequest {
		var txnRequest = ptarmiganpb.KVTxnRequest{}

		if g[0] != nil {
			txnRequest.Compare = *g[0].(*[]*ptarmiganpb.Compare)
		}

		if g[1] != nil {
			txnRequest.Success = *g[1].(*[]*ptarmiganpb.KVRequestOp)
		}

		if g[2] != nil {
			txnRequest.Failure = *g[2].(*[]*ptarmiganpb.KVRequestOp)
		}

		return txnRequest
	}))
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
