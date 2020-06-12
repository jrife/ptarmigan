package gen

import (
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage/model"
	"github.com/jrife/flock/storage/mvcc"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
	"github.com/leanovate/gopter/gen"
)

func abs(n int) int {
	if n < 0 {
		n *= -1
	}

	return n
}

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
		KVQueryRequest(replicaStore),
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
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 25,
			Gen:    gen.Const(mvcc.RevisionOldest),
		},
		{
			Weight: 25,
			Gen:    gen.Const(mvcc.RevisionNewest),
		},
		{
			Weight: 25,
			Gen:    gen.Int64(),
		},
		{
			Weight: 25,
			Gen:    ExistingRevision(replicaStore),
		},
	})
}

func ExistingRevision(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int64().Map(func(n int64) int64 {
		if replicaStore.OldestRevision() == replicaStore.NewestRevision() {
			return n
		}

		return indexFromRange(n, replicaStore.OldestRevision(), replicaStore.NewestRevision())
	})
}

func Value(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen:    gen.Const([]byte(nil)),
		},
		{
			Weight: 65,
			Gen:    AnyValue(),
		},
		{
			Weight: 25,
			Gen:    ExistingValue(replicaStore),
		},
	})
}

func AnyValue() gopter.Gen {
	return gen.AnyString().Map(func(str string) []byte { return []byte(str) })
}

func ExistingValue(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int().Map(func(i int) []byte {
		kvs := replicaStore.Query(ptarmiganpb.KVQueryRequest{}).Kvs

		if len(kvs) == 0 {
			return nil
		}

		return kvs[abs(i)%len(kvs)].Value
	})
}

func Key(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen:    gen.Const([]byte(nil)),
		},
		{
			Weight: 65,
			Gen:    AnyKey(),
		},
		{
			Weight: 25,
			Gen:    ExistingKey(replicaStore),
		},
	})
}

func AnyKey() gopter.Gen {
	return gen.AnyString().Map(func(str string) []byte { return []byte(str) })
}

func ExistingKey(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int().Map(func(i int) []byte {
		kvs := replicaStore.Query(ptarmiganpb.KVQueryRequest{}).Kvs

		if len(kvs) == 0 {
			return nil
		}

		// Weirdness with a byte flipping in the middle of this key
		// maybe somewhere in the bolt driver? Copy the key here
		// to prevent this.
		return append([]byte{}, kvs[abs(i)%len(kvs)].Key...)
	})
}

func KVQueryRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gopter.CombineGens(
		KVSelection(replicaStore),
	).Map(func(g []interface{}) ptarmiganpb.KVQueryRequest {
		var queryRequest ptarmiganpb.KVQueryRequest

		if g[0] != nil {
			queryRequest.Selection = g[0].(*ptarmiganpb.KVSelection)
		}

		queryRequest.After = ""
		queryRequest.Limit = 0
		queryRequest.Revision = 0
		queryRequest.SortOrder = ptarmiganpb.KVQueryRequest_ASC
		queryRequest.SortTarget = ptarmiganpb.KVQueryRequest_KEY

		return queryRequest
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

		if g[0] != nil {
			putRequest.Selection = g[0].(*ptarmiganpb.KVSelection)
		}

		if g[1] != nil {
			putRequest.Key = *g[1].(*[]byte)
		}

		if g[2] != nil {
			putRequest.Value = *g[2].(*[]byte)
		}

		putRequest.Lease = g[3].(int64)
		putRequest.PrevKv = g[4].(bool)
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

		if g[0] != nil {
			deleteRequest.Selection = g[0].(*ptarmiganpb.KVSelection)
		}

		deleteRequest.PrevKv = g[1].(bool)

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
	return gen.PtrOf(gopter.CombineGens(
		KeyRange(replicaStore),
		ModRevisionRange(replicaStore),
		CreateRevisionRange(replicaStore),
		KeyStartsWith(replicaStore),
		SelectionKey(replicaStore),
		Lease(replicaStore),
	).Map(func(g []interface{}) ptarmiganpb.KVSelection {
		var selection ptarmiganpb.KVSelection

		keyRange := g[0].([]interface{})
		modRevisionRange := g[1].([]interface{})
		createRevisionRange := g[2].([]interface{})

		switch keyRange[0].(type) {
		case *ptarmiganpb.KVSelection_KeyGt:
			selection.KeyRangeMin = keyRange[0].(*ptarmiganpb.KVSelection_KeyGt)
		case *ptarmiganpb.KVSelection_KeyGte:
			selection.KeyRangeMin = keyRange[0].(*ptarmiganpb.KVSelection_KeyGte)
		}

		switch keyRange[1].(type) {
		case *ptarmiganpb.KVSelection_KeyLt:
			selection.KeyRangeMax = keyRange[1].(*ptarmiganpb.KVSelection_KeyLt)
		case *ptarmiganpb.KVSelection_KeyLte:
			selection.KeyRangeMax = keyRange[1].(*ptarmiganpb.KVSelection_KeyLte)
		}

		switch modRevisionRange[0].(type) {
		case *ptarmiganpb.KVSelection_ModRevisionGt:
			selection.ModRevisionStart = modRevisionRange[0].(*ptarmiganpb.KVSelection_ModRevisionGt)
		case *ptarmiganpb.KVSelection_ModRevisionGte:
			selection.ModRevisionStart = modRevisionRange[0].(*ptarmiganpb.KVSelection_ModRevisionGte)
		}

		switch modRevisionRange[1].(type) {
		case *ptarmiganpb.KVSelection_ModRevisionLt:
			selection.ModRevisionEnd = modRevisionRange[1].(*ptarmiganpb.KVSelection_ModRevisionLt)
		case *ptarmiganpb.KVSelection_ModRevisionLte:
			selection.ModRevisionEnd = modRevisionRange[1].(*ptarmiganpb.KVSelection_ModRevisionLte)
		}

		switch createRevisionRange[0].(type) {
		case *ptarmiganpb.KVSelection_CreateRevisionGt:
			selection.CreateRevisionStart = createRevisionRange[0].(*ptarmiganpb.KVSelection_CreateRevisionGt)
		case *ptarmiganpb.KVSelection_CreateRevisionGte:
			selection.CreateRevisionStart = createRevisionRange[0].(*ptarmiganpb.KVSelection_CreateRevisionGte)
		}

		switch createRevisionRange[1].(type) {
		case *ptarmiganpb.KVSelection_CreateRevisionLt:
			selection.CreateRevisionEnd = createRevisionRange[1].(*ptarmiganpb.KVSelection_CreateRevisionLt)
		case *ptarmiganpb.KVSelection_CreateRevisionLte:
			selection.CreateRevisionEnd = createRevisionRange[1].(*ptarmiganpb.KVSelection_CreateRevisionLte)
		}

		if g[3] != nil {
			selection.KeyStartsWith = g[3].([]byte)
		}

		if g[4] != nil {
			selection.Key = g[4].([]byte)
		}

		selection.Lease = g[5].(int64)

		return selection
	}))
}

func SelectionKey(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 80,
			Gen:    gen.Const([]byte(nil)),
		},
		{
			Weight: 10,
			Gen:    AnyKey(),
		},
		{
			Weight: 10,
			Gen:    ExistingKey(replicaStore),
		},
	})
}

func KeyRange(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		KeyRangeMin(replicaStore),
		KeyRangeMax(replicaStore),
	)
}

func KeyRangeMin(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		KeyRangeGt(replicaStore),
		KeyRangeGte(replicaStore),
	)
}

func KeyRangeMax(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		KeyRangeLt(replicaStore),
		KeyRangeLte(replicaStore),
	)
}

func KeyRangeGte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KVSelection_KeyGte {
		return ptarmiganpb.KVSelection_KeyGte{KeyGte: b}
	}))
}

func KeyRangeGt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KVSelection_KeyGt {
		return ptarmiganpb.KVSelection_KeyGt{KeyGt: b}
	}))
}

func KeyRangeLte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KVSelection_KeyLte {
		return ptarmiganpb.KVSelection_KeyLte{KeyLte: b}
	}))
}

func KeyRangeLt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KVSelection_KeyLt {
		return ptarmiganpb.KVSelection_KeyLt{KeyLt: b}
	}))
}

func KeyRangeMarker(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 33,
			Gen:    gen.Const([]byte(nil)),
		},
		{
			Weight: 33,
			Gen:    ExistingKey(replicaStore),
		},
		{
			Weight: 33,
			Gen:    AnyKey(),
		},
	})
}

func ModRevisionRange(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		ModRevisionMin(replicaStore),
		ModRevisionMax(replicaStore),
	)
}

func ModRevisionMin(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		ModRevisionGt(replicaStore),
		ModRevisionGte(replicaStore),
	)
}

func ModRevisionMax(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		ModRevisionLt(replicaStore),
		ModRevisionLte(replicaStore),
	)
}

func ModRevisionGte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_ModRevisionGte {
		return ptarmiganpb.KVSelection_ModRevisionGte{ModRevisionGte: r}
	}))
}

func ModRevisionGt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_ModRevisionGt {
		return ptarmiganpb.KVSelection_ModRevisionGt{ModRevisionGt: r}
	}))
}

func ModRevisionLte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_ModRevisionLte {
		return ptarmiganpb.KVSelection_ModRevisionLte{ModRevisionLte: r}
	}))
}

func ModRevisionLt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_ModRevisionLt {
		return ptarmiganpb.KVSelection_ModRevisionLt{ModRevisionLt: r}
	}))
}

func CreateRevisionRange(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		Revision(replicaStore),
		Revision(replicaStore),
	)
}

func CreateRevisionMin(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		CreateRevisionGt(replicaStore),
		CreateRevisionGte(replicaStore),
	)
}

func CreateRevisionMax(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		CreateRevisionLt(replicaStore),
		CreateRevisionLte(replicaStore),
	)
}

func CreateRevisionGte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_CreateRevisionGte {
		return ptarmiganpb.KVSelection_CreateRevisionGte{CreateRevisionGte: r}
	}))
}

func CreateRevisionGt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_CreateRevisionGt {
		return ptarmiganpb.KVSelection_CreateRevisionGt{CreateRevisionGt: r}
	}))
}

func CreateRevisionLte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_CreateRevisionLte {
		return ptarmiganpb.KVSelection_CreateRevisionLte{CreateRevisionLte: r}
	}))
}

func CreateRevisionLt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_CreateRevisionLt {
		return ptarmiganpb.KVSelection_CreateRevisionLt{CreateRevisionLt: r}
	}))
}

func KeyStartsWith(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 25,
			Gen:    gen.Const([]byte(nil)),
		},
		{
			Weight: 25,
			Gen:    gen.AnyString().Map(func(s string) []byte { return []byte(s) }),
		},
		{
			Weight: 50,
			Gen: ExistingKey(replicaStore).Map(func(k []byte) []byte {
				return k[:len(k)/2]
			}),
		},
	})
}

func Lease(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 25,
			Gen:    gen.Int64(),
		},
		{
			Weight: 25,
			Gen:    gen.Const(int64(0)),
		},
		{
			Weight: 50,
			Gen:    ExistingLease(replicaStore),
		},
	})
}

func ExistingLease(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int().Map(func(i int) int64 {
		leases := replicaStore.Leases()

		if len(leases) == 0 {
			return 0
		}

		return leases[abs(i)%len(leases)].ID
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
