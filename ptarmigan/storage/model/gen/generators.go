package gen

import (
	"encoding/base64"
	"encoding/binary"

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
		IndexCommand(replicaStore),
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
	return gopter.CombineGens(
		KVTxnRequest(replicaStore, 0).SuchThat(func(txnRequest *ptarmiganpb.KVTxnRequest) bool {
			return txnRequest != nil
		}),
		Index(replicaStore),
	).Map(func(g []interface{}) commands.Command {
		return txnCommand{
			txnRequest: *g[0].(*ptarmiganpb.KVTxnRequest),
			index:      g[1].(uint64),
		}
	})
}

// CompactCommand returns a generator that generates compact commands
// that are valid for the replica store
func CompactCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		Revision(replicaStore),
		Index(replicaStore),
	).Map(func(g []interface{}) commands.Command {
		return compactCommand{
			revision: g[0].(int64),
			index:    g[1].(uint64),
		}
	})
}

// CreateLeaseCommand returns a generator that generates create lease commands
// that are valid for the replica store
func CreateLeaseCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Int64(),
		Index(replicaStore),
	).Map(func(g []interface{}) commands.Command {
		return createLeaseCommand{
			ttl:   g[0].(int64),
			index: g[1].(uint64),
		}
	})
}

// RevokeLeaseCommand returns a generator that generates revoke lease commands
// that are valid for the replica store
func RevokeLeaseCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		Lease(replicaStore),
		Index(replicaStore),
	).Map(func(g []interface{}) commands.Command {
		return revokeLeaseCommand{
			id:    g[0].(int64),
			index: g[1].(uint64),
		}
	})
}

// LeasesCommand returns a generator that generates leases commands
// that are valid for the replica store
func LeasesCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(leasesCommand{})
}

// GetLeaseCommand returns a generator that generates get lease commands
// that are valid for the replica store
func GetLeaseCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return Lease(replicaStore).Map(func(id int64) commands.Command {
		return getLeaseCommand{ID: id}
	})
}

// NewestRevisionCommand returns a generator that generates newest revision commands
// that are valid for the replica store
func NewestRevisionCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(newestRevisionCommand{})
}

// OldestRevisionCommand returns a generator that generates oldest revision commands
// that are valid for the replica store
func OldestRevisionCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(oldestRevisionCommand{})
}

// IndexCommand returns a generator that generates index commands
// that are valid for the replica store
func IndexCommand(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(indexCommand{})
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

// Revision generates a revision number that may be valid for
// the replica store
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

// ExistingRevision generates valid revision numbers for the replica store
// if it has any revisions
func ExistingRevision(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int64().Map(func(n int64) int64 {
		if replicaStore.OldestRevision() == replicaStore.NewestRevision() {
			return n
		}

		return indexFromRange(n, replicaStore.OldestRevision(), replicaStore.NewestRevision())
	})
}

// Value generates random values and existing values from the replica store
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

// AnyValue generates random values
func AnyValue() gopter.Gen {
	return gen.AnyString().Map(func(str string) []byte { return []byte(str) })
}

// ExistingValue generates values that exist in the replica store
func ExistingValue(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int().Map(func(i int) []byte {
		kvs := replicaStore.Query(ptarmiganpb.KVQueryRequest{}).Kvs

		if len(kvs) == 0 {
			return nil
		}

		return kvs[abs(i)%len(kvs)].Value
	})
}

// Key generates keys, some of which already exist in the replica store
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

// AnyKey generates random keys
func AnyKey() gopter.Gen {
	return gen.AnyString().Map(func(str string) []byte { return []byte(str) })
}

// ExistingKey generates existing keys
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

// KVQueryRequest generates query requests, some of which
// should match some keys in the replica store
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

// After generates a page cursor
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

// SortOrder generates a sort order for a query
func SortOrder() gopter.Gen {
	return gen.OneGenOf(
		gen.Const(ptarmiganpb.KVQueryRequest_ASC),
		gen.Const(ptarmiganpb.KVQueryRequest_DESC),
		gen.Int32().Map(func(i int32) ptarmiganpb.KVQueryRequest_SortOrder { return ptarmiganpb.KVQueryRequest_SortOrder(i) }),
	)
}

// SortTarget generates a sort target for a query
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

// KVPutRequest generates put requests
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

// KVDeleteRequest generates delete requests
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

// KVTxnRequest generates txn requests
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

// SelectionKey generates keys for a selection's key field. It is
// more weighted toward generating a nil key to prevent selections
// from being dominated by those matching only a single key.
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

// KeyRange generates a key range for a selection.
func KeyRange(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		KeyRangeMin(replicaStore),
		KeyRangeMax(replicaStore),
	)
}

// KeyRangeMin generates a key range's minimum value.
func KeyRangeMin(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		KeyRangeGt(replicaStore),
		KeyRangeGte(replicaStore),
	)
}

// KeyRangeMax generates a key range's maximum value.
func KeyRangeMax(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		KeyRangeLt(replicaStore),
		KeyRangeLte(replicaStore),
	)
}

// KeyRangeGte generates a key range min >= some key
func KeyRangeGte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KVSelection_KeyGte {
		return ptarmiganpb.KVSelection_KeyGte{KeyGte: b}
	}))
}

// KeyRangeGt generates a key range max > some key
func KeyRangeGt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KVSelection_KeyGt {
		return ptarmiganpb.KVSelection_KeyGt{KeyGt: b}
	}))
}

// KeyRangeLte generates a key range max <= some key
func KeyRangeLte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KVSelection_KeyLte {
		return ptarmiganpb.KVSelection_KeyLte{KeyLte: b}
	}))
}

// KeyRangeLt generates a key range max < some key
func KeyRangeLt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KVSelection_KeyLt {
		return ptarmiganpb.KVSelection_KeyLt{KeyLt: b}
	}))
}

// KeyRangeMarker generates a key for a key range marker
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

// ModRevisionRange generates a mod revision range for a selection
func ModRevisionRange(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		ModRevisionMin(replicaStore),
		ModRevisionMax(replicaStore),
	)
}

// ModRevisionMin generates a mod revision ranges minimum value
func ModRevisionMin(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		ModRevisionGt(replicaStore),
		ModRevisionGte(replicaStore),
	)
}

// ModRevisionMax generates a mod revision ranges maximum value
func ModRevisionMax(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		ModRevisionLt(replicaStore),
		ModRevisionLte(replicaStore),
	)
}

// ModRevisionGte generates a mod revision range minimum >= some value
func ModRevisionGte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_ModRevisionGte {
		return ptarmiganpb.KVSelection_ModRevisionGte{ModRevisionGte: r}
	}))
}

// ModRevisionGt generates a mod revision range minimum > some value
func ModRevisionGt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_ModRevisionGt {
		return ptarmiganpb.KVSelection_ModRevisionGt{ModRevisionGt: r}
	}))
}

// ModRevisionLte generates a mod revision range maximum <= some value
func ModRevisionLte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_ModRevisionLte {
		return ptarmiganpb.KVSelection_ModRevisionLte{ModRevisionLte: r}
	}))
}

// ModRevisionLt generates a mod revision range maximum < some value
func ModRevisionLt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_ModRevisionLt {
		return ptarmiganpb.KVSelection_ModRevisionLt{ModRevisionLt: r}
	}))
}

// CreateRevisionRange generates a create revision range for a selection
func CreateRevisionRange(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		Revision(replicaStore),
		Revision(replicaStore),
	)
}

// CreateRevisionMin generates a create revision ranges minimum value
func CreateRevisionMin(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		CreateRevisionGt(replicaStore),
		CreateRevisionGte(replicaStore),
	)
}

// CreateRevisionMax generates a create revision ranges maximum value
func CreateRevisionMax(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		CreateRevisionLt(replicaStore),
		CreateRevisionLte(replicaStore),
	)
}

// CreateRevisionGte generates a create revision range minimum >= some value
func CreateRevisionGte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_CreateRevisionGte {
		return ptarmiganpb.KVSelection_CreateRevisionGte{CreateRevisionGte: r}
	}))
}

// CreateRevisionGt generates a create revision range minimum > some value
func CreateRevisionGt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_CreateRevisionGt {
		return ptarmiganpb.KVSelection_CreateRevisionGt{CreateRevisionGt: r}
	}))
}

// CreateRevisionLte generates a create revision range maximum <= some value
func CreateRevisionLte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_CreateRevisionLte {
		return ptarmiganpb.KVSelection_CreateRevisionLte{CreateRevisionLte: r}
	}))
}

// CreateRevisionLt generates a create revision range maximum <= some value
func CreateRevisionLt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Revision(replicaStore).Map(func(r int64) ptarmiganpb.KVSelection_CreateRevisionLt {
		return ptarmiganpb.KVSelection_CreateRevisionLt{CreateRevisionLt: r}
	}))
}

// KeyStartsWith generates a key prefix for a selection's KeyStartsWith field
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

// Lease generates a lease and is weighted toward generating an existing lease
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

// ExistingLease generates an existing lease if one exists, 0 otherwise.
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

// Comparison generates a KV predicate's comparison field
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

// ValidComparison generates a comparison that has a valid value
func ValidComparison() gopter.Gen {
	return gen.Int32Range(int32(ptarmiganpb.KVPredicate_EQUAL), int32(ptarmiganpb.KVPredicate_NOT_EQUAL)).Map(func(i int32) ptarmiganpb.KVPredicate_Comparison {
		return ptarmiganpb.KVPredicate_Comparison(i)
	})
}

// Target generates the target for a comparison in a KV predicate's target and target union fields.
func Target(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		AnyTarget(replicaStore),
		ValidTarget(replicaStore),
	)
}

// AnyTarget generates a random combination of target and target union
func AnyTarget(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Int32Range(int32(ptarmiganpb.KVPredicate_VERSION), int32(ptarmiganpb.KVPredicate_LEASE)).Map(func(i int32) ptarmiganpb.KVPredicate_Target {
			return ptarmiganpb.KVPredicate_Target(i)
		}),
		TargetUnion(replicaStore),
	)
}

// ValidTarget generates a valid combination of target and target union
func ValidTarget(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return TargetUnion(replicaStore).Map(func(targetUnion interface{}) []interface{} {
		switch targetUnion.(*gopter.GenResult).Result.(type) {
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

// TargetUnion generates a target union, and sometimes nil
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

// KVPredicate_Version generates a version for the target union field of a predicate
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

// ExistingVersion generates a version for the target union field of a predicate
// that matches an existing key version
func ExistingVersion(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int().Map(func(i int) ptarmiganpb.KVPredicate_Version {
		kvs := replicaStore.Query(ptarmiganpb.KVQueryRequest{}).Kvs

		if len(kvs) == 0 {
			return ptarmiganpb.KVPredicate_Version{}
		}

		return ptarmiganpb.KVPredicate_Version{Version: kvs[abs(i)%len(kvs)].Version}
	})
}

// KVPredicate_ModRevision generates a mod revision for the target union field of a predicate
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

// KVPredicate_CreateRevision generates a create revision for the target union field of a predicate
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

// KVPredicate_Lease generates a lease for the target union field of a predicate
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

// KVPredicate_Value generates a value for the target union field of a predicate
func KVPredicate_Value(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(Value(replicaStore).Map(func(value []byte) ptarmiganpb.KVPredicate_Value {
		return ptarmiganpb.KVPredicate_Value{Value: value}
	}))
}

// Index generates indexes for update commands
func Index(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int64Range(-1, 40).Map(func(i int64) uint64 {
		index := replicaStore.Index()

		if i < 0 && index < uint64(i) {
			return index - uint64(-1*i)
		} else if i > 0 {
			return index + uint64(i)
		}

		return index
	})
}
