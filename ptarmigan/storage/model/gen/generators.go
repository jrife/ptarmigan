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
// of storage commands that are valid for the set of replica stores
func Commands(replicaStoreSet []model.ReplicaStoreModel) gopter.Gen {
	gens := make([]gopter.Gen, len(replicaStoreSet)+1)

	for i := range replicaStoreSet {
		gens[i] = CommandsOne(i, &replicaStoreSet[i])
	}

	gens[len(gens)-1] = SnapshotCommand(replicaStoreSet)

	return gen.OneGenOf(gens...)
}

// CommandsOne returns a generator that generates a range
// of storage commands that are valid for the replica store
func CommandsOne(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		QueryCommand(id, replicaStore),
		ChangesCommand(id, replicaStore),
		TxnCommand(id, replicaStore),
		CompactCommand(id, replicaStore),
		CreateLeaseCommand(id, replicaStore),
		RevokeLeaseCommand(id, replicaStore),
		LeasesCommand(id, replicaStore),
		GetLeaseCommand(id, replicaStore),
		NewestRevisionCommand(id, replicaStore),
		OldestRevisionCommand(id, replicaStore),
		IndexCommand(id, replicaStore),
	)
}

// QueryCommand returns a generator that generates query commands
// that are valid for the replica store
func QueryCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return KVQueryRequest(replicaStore).SuchThat(func(queryRequest *ptarmiganpb.KVQueryRequest) bool {
		return queryRequest != nil
	}).Map(func(queryRequest *ptarmiganpb.KVQueryRequest) commands.Command {
		return queryCommand{
			queryRequest: *queryRequest,
			command: command{
				replicaStore: id,
			},
		}
	})
}

// ChangesCommand returns a generator that generates changes commands
// that are valid for the replica store
func ChangesCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		KVWatchRequest(replicaStore),
		gen.IntRange(-1, 100),
	).Map(func(g []interface{}) commands.Command {
		result := changesCommand{
			command: command{
				replicaStore: id,
			},
			limit:        g[1].(int),
			watchRequest: ptarmiganpb.KVWatchRequest{},
		}

		if g[0] != nil {
			result.watchRequest = *g[0].(*ptarmiganpb.KVWatchRequest)
		}

		return result
	})
}

// TxnCommand returns a generator that generates txn commands
// that are valid for the replica store
func TxnCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		KVTxnRequest(replicaStore, 0).SuchThat(func(txnRequest *ptarmiganpb.KVTxnRequest) bool {
			return txnRequest != nil
		}),
		Index(replicaStore),
	).Map(func(g []interface{}) commands.Command {
		return txnCommand{
			txnRequest: *g[0].(*ptarmiganpb.KVTxnRequest),
			index:      g[1].(uint64),
			command: command{
				replicaStore: id,
			},
		}
	})
}

// CompactCommand returns a generator that generates compact commands
// that are valid for the replica store
func CompactCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		Revision(replicaStore),
		Index(replicaStore),
	).Map(func(g []interface{}) commands.Command {
		return compactCommand{
			revision: g[0].(int64),
			index:    g[1].(uint64),
			command: command{
				replicaStore: id,
			},
		}
	})
}

// CreateLeaseCommand returns a generator that generates create lease commands
// that are valid for the replica store
func CreateLeaseCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Int64(),
		Index(replicaStore),
	).Map(func(g []interface{}) commands.Command {
		return createLeaseCommand{
			ttl:   g[0].(int64),
			index: g[1].(uint64),
			command: command{
				replicaStore: id,
			},
		}
	})
}

// RevokeLeaseCommand returns a generator that generates revoke lease commands
// that are valid for the replica store
func RevokeLeaseCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		Lease(replicaStore),
		Index(replicaStore),
	).Map(func(g []interface{}) commands.Command {
		return revokeLeaseCommand{
			id:    g[0].(int64),
			index: g[1].(uint64),
			command: command{
				replicaStore: id,
			},
		}
	})
}

// LeasesCommand returns a generator that generates leases commands
// that are valid for the replica store
func LeasesCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(leasesCommand{
		command: command{
			replicaStore: id,
		},
	})
}

// GetLeaseCommand returns a generator that generates get lease commands
// that are valid for the replica store
func GetLeaseCommand(replicaStoreID int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return Lease(replicaStore).Map(func(id int64) commands.Command {
		return getLeaseCommand{
			id: id,
			command: command{
				replicaStore: replicaStoreID,
			},
		}
	})
}

// NewestRevisionCommand returns a generator that generates newest revision commands
// that are valid for the replica store
func NewestRevisionCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(newestRevisionCommand{
		command: command{
			replicaStore: id,
		},
	})
}

// OldestRevisionCommand returns a generator that generates oldest revision commands
// that are valid for the replica store
func OldestRevisionCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(oldestRevisionCommand{
		command: command{
			replicaStore: id,
		},
	})
}

// IndexCommand returns a generator that generates index commands
// that are valid for the replica store
func IndexCommand(id int, replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Const(indexCommand{
		command: command{
			replicaStore: id,
		},
	})
}

// SnapshotCommand returns a generator that generates snapshot commands
func SnapshotCommand(replicaStores []model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(0, len(replicaStores)-1),
		gen.IntRange(0, len(replicaStores)-1),
	).SuchThat(func(g []interface{}) bool {
		return g[0].(int) != g[1].(int)
	}).Map(func(g []interface{}) commands.Command {
		return snapshotCommand{
			source: g[0].(int),
			dest:   g[1].(int),
		}
	})
}

// Condition returns a generator that generates a list of conditions that are
// valid for the replica store
func Condition(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		KVSelection(replicaStore),
		KVPredicate(replicaStore),
		gen.Int32Range(0, 5),
	).Map(func(g []interface{}) ptarmiganpb.Condition {
		var condition ptarmiganpb.Condition

		if g[0] != nil {
			condition.Domain = g[0].([]ptarmiganpb.KVPredicate)
		}

		if g[1] != nil {
			condition.Predicate = g[1].(ptarmiganpb.KVPredicate)
		}

		condition.Quantifier = ptarmiganpb.Condition_Quantifier(g[2].(int32))

		return condition
	})
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
	}).Map(func(g *gopter.GenResult) ptarmiganpb.KVRequestOp {
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

		return requestOp
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

// KVWatchRequest generates watch requests, some of which
// should match some keys in the replica store
func KVWatchRequest(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gopter.CombineGens(
		KeyRange(replicaStore),
		KVWatchCursor(replicaStore),
		gen.Bool(),
		gen.Bool(),
		gen.Bool(),
	).Map(func(g []interface{}) ptarmiganpb.KVWatchRequest {
		var watchRequest ptarmiganpb.KVWatchRequest

		if g[0] != nil {
			watchRequest.Keys = g[0].(ptarmiganpb.KeyRange)
		}

		if g[1] != nil {
			watchRequest.Start = *g[1].(*ptarmiganpb.KVWatchCursor)
		}

		watchRequest.NoDelete = g[2].(bool)
		watchRequest.NoPut = g[3].(bool)
		watchRequest.PrevKv = g[4].(bool)

		return watchRequest
	}))
}

// KVWatchCursor generates a watch cursor
func KVWatchCursor(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(gopter.CombineGens(
		Revision(replicaStore),
		Key(replicaStore),
	).Map(func(g []interface{}) ptarmiganpb.KVWatchCursor {
		var watchCursor ptarmiganpb.KVWatchCursor

		watchCursor.Revision = g[0].(int64)

		if g[1] != nil {
			watchCursor.Key = g[1].([]byte)
		}

		return watchCursor
	}))
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
			queryRequest.Selection = g[0].([]ptarmiganpb.KVPredicate)
		}

		queryRequest.Limit = g[1].(int64)
		queryRequest.Revision = g[2].(int64)
		queryRequest.SortOrder = g[3].(ptarmiganpb.KVQueryRequest_SortOrder)
		queryRequest.SortTarget = g[4].(ptarmiganpb.Field)
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
			ExistingVersion(replicaStore),
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
		gen.Const(ptarmiganpb.ASC),
		gen.Const(ptarmiganpb.DESC),
		gen.Int32().Map(func(i int32) ptarmiganpb.KVQueryRequest_SortOrder { return ptarmiganpb.KVQueryRequest_SortOrder(i) }),
	)
}

// SortTarget generates a sort target for a query
func SortTarget() gopter.Gen {
	return gen.OneGenOf(
		gen.Const(ptarmiganpb.KEY),
		gen.Const(ptarmiganpb.VERSION),
		gen.Const(ptarmiganpb.CREATE),
		gen.Const(ptarmiganpb.MOD),
		gen.Const(ptarmiganpb.LEASE),
		gen.Const(ptarmiganpb.VALUE),
		gen.Int32().Map(func(i int32) ptarmiganpb.Field { return ptarmiganpb.Field(i) }),
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
			putRequest.Selection = g[0].([]ptarmiganpb.KVPredicate)
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
			deleteRequest.Selection = g[0].([]ptarmiganpb.KVPredicate)
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
		gen.PtrOf(gen.SliceOf(Condition(replicaStore))),
		gen.PtrOf(gen.SliceOf(KVRequestOp(replicaStore, depth))),
		gen.PtrOf(gen.SliceOf(KVRequestOp(replicaStore, depth))),
	).Map(func(g []interface{}) ptarmiganpb.KVTxnRequest {
		var txnRequest = ptarmiganpb.KVTxnRequest{}

		if g[0] != nil {
			txnRequest.Conditions = *g[0].(*[]ptarmiganpb.Condition)
		}

		if g[1] != nil {
			txnRequest.Success = *g[1].(*[]ptarmiganpb.KVRequestOp)
		}

		if g[2] != nil {
			txnRequest.Failure = *g[2].(*[]ptarmiganpb.KVRequestOp)
		}

		return txnRequest
	}))
}

// KVSelection returns a generator that generates selections that are
// valid for the replica store. The majority of possible randomly
// generated selections wouldn't match anything. This generates selections
// in a way that will increase the chances of a match.
func KVSelection(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.SliceOf(KVPredicate(replicaStore))
}

// KeyRangeKey generates keys for a key range's key field. It is
// more weighted toward generating a nil key to prevent key ranges
// from being dominated by those matching only a single key.
func KeyRangeKey(replicaStore *model.ReplicaStoreModel) gopter.Gen {
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
		KeyRangeKey(replicaStore),
		KeyStartsWith(replicaStore),
	).Map(func(g []interface{}) ptarmiganpb.KeyRange {
		var keyRange ptarmiganpb.KeyRange

		switch g[0].(type) {
		case *ptarmiganpb.KeyRange_KeyGt:
			keyRange.KeyRangeMin = g[0].(*ptarmiganpb.KeyRange_KeyGt)
		case *ptarmiganpb.KeyRange_KeyGte:
			keyRange.KeyRangeMin = g[0].(*ptarmiganpb.KeyRange_KeyGte)
		}

		switch g[1].(type) {
		case *ptarmiganpb.KeyRange_KeyLt:
			keyRange.KeyRangeMax = g[1].(*ptarmiganpb.KeyRange_KeyLt)
		case *ptarmiganpb.KeyRange_KeyLte:
			keyRange.KeyRangeMax = g[1].(*ptarmiganpb.KeyRange_KeyLte)
		}

		if g[2] != nil {
			keyRange.Key = g[2].([]byte)
		}

		if g[3] != nil {
			keyRange.KeyStartsWith = g[3].([]byte)
		}

		return keyRange
	})
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
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KeyRange_KeyGte {
		return ptarmiganpb.KeyRange_KeyGte{KeyGte: b}
	}))
}

// KeyRangeGt generates a key range max > some key
func KeyRangeGt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KeyRange_KeyGt {
		return ptarmiganpb.KeyRange_KeyGt{KeyGt: b}
	}))
}

// KeyRangeLte generates a key range max <= some key
func KeyRangeLte(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KeyRange_KeyLte {
		return ptarmiganpb.KeyRange_KeyLte{KeyLte: b}
	}))
}

// KeyRangeLt generates a key range max < some key
func KeyRangeLt(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.PtrOf(KeyRangeMarker(replicaStore).Map(func(b []byte) ptarmiganpb.KeyRange_KeyLt {
		return ptarmiganpb.KeyRange_KeyLt{KeyLt: b}
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
	return gopter.CombineGens(
		Operator(),
		Target(replicaStore),
	).Map(func(g []interface{}) ptarmiganpb.KVPredicate {
		var predicate ptarmiganpb.KVPredicate

		predicate.Operator = g[0].(ptarmiganpb.KVPredicate_Operator)

		target := g[1].([]interface{})

		predicate.Field = target[0].(ptarmiganpb.Field)

		switch target[1].(type) {
		case *ptarmiganpb.KVPredicate_Bytes:
			predicate.Value = target[1].(*ptarmiganpb.KVPredicate_Bytes)

			// existing keys are selected without truncation. We need to truncate to be a true prefix
			if predicate.Operator == ptarmiganpb.STARTS_WITH && predicate.Value.(*ptarmiganpb.KVPredicate_Bytes).Bytes != nil {
				bytes := predicate.Value.(*ptarmiganpb.KVPredicate_Bytes).Bytes
				predicate.Value.(*ptarmiganpb.KVPredicate_Bytes).Bytes = bytes[:len(bytes)/2]
			}

		case *ptarmiganpb.KVPredicate_Int64:
			predicate.Value = target[1].(*ptarmiganpb.KVPredicate_Int64)
		}

		return predicate
	})
}

// Operator generates a KV predicate's operator field
func Operator() gopter.Gen {
	return gen.Weighted([]gen.WeightedGen{
		{
			Weight: 10,
			Gen:    gen.Int32().Map(func(i int32) ptarmiganpb.KVPredicate_Operator { return ptarmiganpb.KVPredicate_Operator(i) }),
		},
		{
			Weight: 90,
			Gen:    ValidOperator(),
		},
	})
}

// ValidOperator generates a comparison that has a valid value
func ValidOperator() gopter.Gen {
	return gen.Int32Range(int32(ptarmiganpb.EQUAL), int32(ptarmiganpb.STARTS_WITH)).Map(func(i int32) ptarmiganpb.KVPredicate_Operator {
		return ptarmiganpb.KVPredicate_Operator(i)
	})
}

// Target generates the target for a comparison in a KV predicate's field and value fields.
func Target(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		AnyTarget(replicaStore),
		ValidTarget(replicaStore),
	)
}

// AnyTarget generates a random combination of field and value
func AnyTarget(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Int32Range(int32(ptarmiganpb.VERSION), int32(ptarmiganpb.KEY)).Map(func(i int32) ptarmiganpb.Field {
			return ptarmiganpb.Field(i)
		}),
		gen.OneGenOf(
			gen.Const(nil),
			AnyKey().Map(func(k []byte) *ptarmiganpb.KVPredicate_Bytes { return &ptarmiganpb.KVPredicate_Bytes{Bytes: k} }),
			gen.Int64().Map(func(i int64) *ptarmiganpb.KVPredicate_Int64 { return &ptarmiganpb.KVPredicate_Int64{Int64: i} }),
		),
	)
}

// ValidTarget generates a valid combination of target and target union
func ValidTarget(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.OneGenOf(
		KVPredicate_Version(replicaStore),
		KVPredicate_ModRevision(replicaStore),
		KVPredicate_CreateRevision(replicaStore),
		KVPredicate_Lease(replicaStore),
		KVPredicate_Key(replicaStore),
		KVPredicate_Value(replicaStore),
	)
}

// KVPredicate_Version generates a version for the target union field of a predicate
func KVPredicate_Version(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Const(ptarmiganpb.VERSION),
		gen.PtrOf(gen.Weighted([]gen.WeightedGen{
			{
				Weight: 10,
				Gen: gen.Int64().Map(func(i int64) ptarmiganpb.KVPredicate_Int64 {
					return ptarmiganpb.KVPredicate_Int64{Int64: i}
				}),
			},
			{
				Weight: 90,
				Gen:    ExistingVersion(replicaStore).Map(func(i int64) ptarmiganpb.KVPredicate_Int64 { return ptarmiganpb.KVPredicate_Int64{Int64: i} }),
			},
		})),
	)
}

// ExistingVersion generates a version for the target union field of a predicate
// that matches an existing key version
func ExistingVersion(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gen.Int().Map(func(i int) int64 {
		kvs := replicaStore.Query(ptarmiganpb.KVQueryRequest{}).Kvs

		if len(kvs) == 0 {
			return 0
		}

		return kvs[abs(i)%len(kvs)].Version
	})
}

// KVPredicate_ModRevision generates a mod revision for the target union field of a predicate
func KVPredicate_ModRevision(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Const(ptarmiganpb.MOD),
		gen.PtrOf(gen.Weighted([]gen.WeightedGen{
			{
				Weight: 10,
				Gen: gen.Int64().Map(func(i int64) ptarmiganpb.KVPredicate_Int64 {
					return ptarmiganpb.KVPredicate_Int64{Int64: i}
				}),
			},
			{
				Weight: 90,
				Gen:    ExistingRevision(replicaStore).Map(func(i int64) ptarmiganpb.KVPredicate_Int64 { return ptarmiganpb.KVPredicate_Int64{Int64: i} }),
			},
		})),
	)
}

// KVPredicate_CreateRevision generates a create revision for the target union field of a predicate
func KVPredicate_CreateRevision(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Const(ptarmiganpb.CREATE),
		gen.PtrOf(gen.Weighted([]gen.WeightedGen{
			{
				Weight: 10,
				Gen: gen.Int64().Map(func(i int64) ptarmiganpb.KVPredicate_Int64 {
					return ptarmiganpb.KVPredicate_Int64{Int64: i}
				}),
			},
			{
				Weight: 90,
				Gen:    ExistingRevision(replicaStore).Map(func(i int64) ptarmiganpb.KVPredicate_Int64 { return ptarmiganpb.KVPredicate_Int64{Int64: i} }),
			},
		})),
	)
}

// KVPredicate_Lease generates a lease for the target union field of a predicate
func KVPredicate_Lease(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Const(ptarmiganpb.LEASE),
		gen.PtrOf(gen.Weighted([]gen.WeightedGen{
			{
				Weight: 10,
				Gen: gen.Int64().Map(func(i int64) ptarmiganpb.KVPredicate_Int64 {
					return ptarmiganpb.KVPredicate_Int64{Int64: i}
				}),
			},
			{
				Weight: 90,
				Gen:    ExistingLease(replicaStore).Map(func(i int64) ptarmiganpb.KVPredicate_Int64 { return ptarmiganpb.KVPredicate_Int64{Int64: i} }),
			},
		})),
	)
}

// KVPredicate_Key generates a value for the target union field of a predicate
func KVPredicate_Key(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Const(ptarmiganpb.KEY),
		gen.PtrOf(Key(replicaStore).Map(func(b []byte) ptarmiganpb.KVPredicate_Bytes { return ptarmiganpb.KVPredicate_Bytes{Bytes: b} })),
	)
}

// KVPredicate_Value generates a value for the target union field of a predicate
func KVPredicate_Value(replicaStore *model.ReplicaStoreModel) gopter.Gen {
	return gopter.CombineGens(
		gen.Const(ptarmiganpb.VALUE),
		gen.PtrOf(Value(replicaStore).Map(func(b []byte) ptarmiganpb.KVPredicate_Bytes { return ptarmiganpb.KVPredicate_Bytes{Bytes: b} })),
	)
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
