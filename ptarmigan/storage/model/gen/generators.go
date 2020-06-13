package gen

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"

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
		CreateLeaseCommand(replicaStore),
		RevokeLeaseCommand(replicaStore),
		LeasesCommand(replicaStore),
		GetLeaseCommand(replicaStore),
		NewestRevisionCommand(replicaStore),
		OldestRevisionCommand(replicaStore),
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
	return KVTxnRequest(replicaStore, 0).SuchThat(func(txnRequest *ptarmiganpb.KVTxnRequest) bool {
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

func CreateLeaseCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int64().Map(func(n int64) commands.Command {
		return createLeaseCommand{TTL: n}
	})
}

func RevokeLeaseCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return Lease(replicaStore).Map(func(id int64) commands.Command {
		return revokeLeaseCommand{ID: id}
	})
}

func LeasesCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(leasesCommand{})
}

func GetLeaseCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return Lease(replicaStore).Map(func(id int64) commands.Command {
		return getLeaseCommand{ID: id}
	})
}

func NewestRevisionCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(newestRevisionCommand{})
}

func OldestRevisionCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(oldestRevisionCommand{})
}

// Compare returns a generator that generates comparisons that are
// valid for the replica store
func Compare(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gopter.CombineGens(
		KVSelection(replicaStore),
		KVPredicate(replicaStore),
	).Map(func(g []interface{}) ptarmiganpb.Compare {
		var compare ptarmiganpb.Compare

		if g[0] != nil {
			compare.Selection = g[0].(*ptarmiganpb.KVSelection)
		}

		if g[1] != nil {
			compare.Predicate = g[1].(*ptarmiganpb.KVPredicate)
		}

		return compare
	}))
}

// KVRequestOp returns a generator that generates request ops that are
// valid for the replica store
func KVRequestOp(replicaStore *model.ReplicaStoreModel, depth int) gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen:    KVTxnRequest(replicaStore, depth+1),
		},
		{
			Weight: 50,
			Gen:    KVQueryRequest(replicaStore),
		},
		{
			Weight: 50,
			Gen:    KVPutRequest(replicaStore),
		},
		{
			Weight: 50,
			Gen:    KVDeleteRequest(replicaStore),
		},
	}).Map(func(g *gopter.GenResult) *ptarmiganpb.KVRequestOp {
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
		gen.Int64Range(-1, 100),
		Revision(replicaStore),
		SortOrder(),
		SortTarget(),
		gen.Bool(),
		gen.Bool(),
		After(replicaStore),
	).Map(func(g []interface{}) ptarmiganpb.KVQueryRequest {
		var queryRequest ptarmiganpb.KVQueryRequest

		if g[0] != nil {
			queryRequest.Selection = g[0].(*ptarmiganpb.KVSelection)
		}

		queryRequest.Limit = g[1].(int64)
		queryRequest.Revision = g[2].(int64)
		queryRequest.SortOrder = g[3].(ptarmiganpb.KVQueryRequest_SortOrder)
		queryRequest.SortTarget = g[4].(ptarmiganpb.KVQueryRequest_SortTarget)
		queryRequest.IncludeCount = g[5].(bool)
		queryRequest.ExcludeValues = g[6].(bool)
		queryRequest.After = g[7].(string)

		return queryRequest
	}))
}

func After(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		gen.OneGenOf(
			Key(replicaStore),
			Value(replicaStore),
		).Map(func(g interface{}) []byte {
			return g.(*gopter.GenResult).Result.([]byte)
		}),
		gen.OneGenOf(
			ExistingVersion(replicaStore).Map(func(v ptarmiganpb.KVPredicate_Version) int64 { return v.Version }),
			ExistingRevision(replicaStore),
			gen.Int64(),
		).Map(func(g interface{}) []byte {
			i := g.(*gopter.GenResult).Result.(int64)
			b := make([]byte, 8)

			binary.BigEndian.PutUint64(b, uint64(i))

			return b
		}),
	).Map(func(b []byte) string {
		return base64.StdEncoding.EncodeToString(b)
	})
}

func SortOrder() gopter.Gen {
	return gen.OneGenOf(
		gen.Const(ptarmiganpb.KVQueryRequest_ASC),
		gen.Const(ptarmiganpb.KVQueryRequest_DESC),
		gen.Int32().Map(func(i int32) ptarmiganpb.KVQueryRequest_SortOrder { return ptarmiganpb.KVQueryRequest_SortOrder(i) }),
	)
}

func SortTarget() gopter.Gen {
	return gen.OneGenOf(
		gen.Const(ptarmiganpb.KVQueryRequest_KEY),
		gen.Const(ptarmiganpb.KVQueryRequest_VERSION),
		gen.Const(ptarmiganpb.KVQueryRequest_CREATE),
		gen.Const(ptarmiganpb.KVQueryRequest_MOD),
		gen.Const(ptarmiganpb.KVQueryRequest_VALUE),
		gen.Int32().Map(func(i int32) ptarmiganpb.KVQueryRequest_SortTarget { return ptarmiganpb.KVQueryRequest_SortTarget(i) }),
	)
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
		fmt.Printf("Gen\n")
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

		if g[3].(int64) != 0 {
			fmt.Printf("Lease %d\n", g[3].(int64))
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

func KVTxnRequest(replicaStore *model.ReplicaStoreModel, depth int) gopter.Gen {
	if depth > 1 {
		return gen.Const(nil)
	}

	return gen.PtrOf(gopter.CombineGens(
		gen.PtrOf(gen.SliceOf(Compare(replicaStore))),
		gen.PtrOf(gen.SliceOf(KVRequestOp(replicaStore, depth))),
		gen.PtrOf(gen.SliceOf(KVRequestOp(replicaStore, depth))),
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
	return gen.PtrOf(gopter.CombineGens(
		Comparison(),
		Target(replicaStore),
	).Map(func(g []interface{}) ptarmiganpb.KVPredicate {
		var predicate ptarmiganpb.KVPredicate
		target := g[1].([]interface{})

		predicate.Comparison = g[0].(ptarmiganpb.KVPredicate_Comparison)
		predicate.Target = target[0].(ptarmiganpb.KVPredicate_Target)

		switch target[1].(type) {
		case *ptarmiganpb.KVPredicate_CreateRevision:
			predicate.TargetUnion = target[1].(*ptarmiganpb.KVPredicate_CreateRevision)
		case *ptarmiganpb.KVPredicate_ModRevision:
			predicate.TargetUnion = target[1].(*ptarmiganpb.KVPredicate_ModRevision)
		case *ptarmiganpb.KVPredicate_Version:
			predicate.TargetUnion = target[1].(*ptarmiganpb.KVPredicate_Version)
		case *ptarmiganpb.KVPredicate_Lease:
			predicate.TargetUnion = target[1].(*ptarmiganpb.KVPredicate_Lease)
		case *ptarmiganpb.KVPredicate_Value:
			predicate.TargetUnion = target[1].(*ptarmiganpb.KVPredicate_Value)
		}

		return predicate
	}))
}

func Comparison() gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen:    gen.Int32().Map(func(i int32) ptarmiganpb.KVPredicate_Comparison { return ptarmiganpb.KVPredicate_Comparison(i) }),
		},
		{
			Weight: 90,
			Gen:    ValidComparison(),
		},
	})
}

func ValidComparison() gopter.Gen {
	return gen.Int32Range(int32(ptarmiganpb.KVPredicate_EQUAL), int32(ptarmiganpb.KVPredicate_NOT_EQUAL)).Map(func(i int32) ptarmiganpb.KVPredicate_Comparison {
		return ptarmiganpb.KVPredicate_Comparison(i)
	})
}

func Target(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		AnyTarget(replicaStore),
		ValidTarget(replicaStore),
	)
}

func AnyTarget(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Int32Range(int32(ptarmiganpb.KVPredicate_VERSION), int32(ptarmiganpb.KVPredicate_LEASE)).Map(func(i int32) ptarmiganpb.KVPredicate_Target {
			return ptarmiganpb.KVPredicate_Target(i)
		}),
		TargetUnion(replicaStore),
	)
}

func ValidTarget(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return TargetUnion(replicaStore).Map(func(targetUnion interface{}) []interface{} {
		switch targetUnion.(type) {
		case *ptarmiganpb.KVPredicate_CreateRevision:
			return []interface{}{ptarmiganpb.KVPredicate_CREATE, targetUnion.(*gopter.GenResult).Result}
		case *ptarmiganpb.KVPredicate_ModRevision:
			return []interface{}{ptarmiganpb.KVPredicate_MOD, targetUnion.(*gopter.GenResult).Result}
		case *ptarmiganpb.KVPredicate_Version:
			return []interface{}{ptarmiganpb.KVPredicate_VERSION, targetUnion.(*gopter.GenResult).Result}
		case *ptarmiganpb.KVPredicate_Lease:
			return []interface{}{ptarmiganpb.KVPredicate_LEASE, targetUnion.(*gopter.GenResult).Result}
		case *ptarmiganpb.KVPredicate_Value:
			return []interface{}{ptarmiganpb.KVPredicate_VALUE, targetUnion.(*gopter.GenResult).Result}
		}

		return []interface{}{ptarmiganpb.KVPredicate_Target(0), targetUnion.(*gopter.GenResult).Result}
	})
}

func TargetUnion(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen:    gen.Const(nil),
		},
		{
			Weight: 90,
			Gen: gen.OneGenOf(
				KVPredicate_Version(replicaStore),
				KVPredicate_ModRevision(replicaStore),
				KVPredicate_CreateRevision(replicaStore),
				KVPredicate_Lease(replicaStore),
				KVPredicate_Value(replicaStore),
			),
		},
	})
}

func KVPredicate_Version(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen: gen.Int64().Map(func(i int64) ptarmiganpb.KVPredicate_Version {
				return ptarmiganpb.KVPredicate_Version{Version: i}
			}),
		},
		{
			Weight: 90,
			Gen:    ExistingVersion(replicaStore),
		},
	}))
}

func ExistingVersion(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int().Map(func(i int) ptarmiganpb.KVPredicate_Version {
		kvs := replicaStore.Query(ptarmiganpb.KVQueryRequest{}).Kvs

		if len(kvs) == 0 {
			return ptarmiganpb.KVPredicate_Version{}
		}

		return ptarmiganpb.KVPredicate_Version{Version: kvs[abs(i)%len(kvs)].Version}
	})
}

func KVPredicate_ModRevision(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen: gen.Int64().Map(func(i int64) ptarmiganpb.KVPredicate_ModRevision {
				return ptarmiganpb.KVPredicate_ModRevision{ModRevision: i}
			}),
		},
		{
			Weight: 90,
			Gen: ExistingRevision(replicaStore).Map(func(i int64) ptarmiganpb.KVPredicate_ModRevision {
				return ptarmiganpb.KVPredicate_ModRevision{ModRevision: i}
			}),
		},
	}))
}

func KVPredicate_CreateRevision(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen: gen.Int64().Map(func(i int64) ptarmiganpb.KVPredicate_CreateRevision {
				return ptarmiganpb.KVPredicate_CreateRevision{CreateRevision: i}
			}),
		},
		{
			Weight: 90,
			Gen: ExistingRevision(replicaStore).Map(func(i int64) ptarmiganpb.KVPredicate_CreateRevision {
				return ptarmiganpb.KVPredicate_CreateRevision{CreateRevision: i}
			}),
		},
	}))
}

func KVPredicate_Lease(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen: gen.Int64().Map(func(i int64) ptarmiganpb.KVPredicate_Lease {
				return ptarmiganpb.KVPredicate_Lease{Lease: i}
			}),
		},
		{
			Weight: 90,
			Gen: ExistingLease(replicaStore).Map(func(i int64) ptarmiganpb.KVPredicate_Lease {
				return ptarmiganpb.KVPredicate_Lease{Lease: i}
			}),
		},
	}))
}

func KVPredicate_Value(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Value(replicaStore).Map(func(value []byte) ptarmiganpb.KVPredicate_Value {
		return ptarmiganpb.KVPredicate_Value{Value: value}
	}))
}
