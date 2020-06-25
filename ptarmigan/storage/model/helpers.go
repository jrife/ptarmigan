package model

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"sort"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/mvcc"
)

type kvList []ptarmiganpb.KeyValue

func (kvs kvList) selection(selection []ptarmiganpb.KVPredicate) kvList {
	sel := []ptarmiganpb.KeyValue{}

	for _, kv := range kvs {
		if KVMatchesSelection(selection, kv) {
			sel = append(sel, kv)
		}
	}

	return sel
}

func (kvs kvList) filter(predicate ptarmiganpb.KVPredicate) kvList {
	filtered := []ptarmiganpb.KeyValue{}

	for _, kv := range kvs {
		if KVMatchesPredicate(predicate, kv) {
			filtered = append(filtered, kv)
		}
	}

	return filtered
}

type eventList []ptarmiganpb.Event

func (events eventList) filter(keyRange ptarmiganpb.KeyRange) eventList {
	filtered := []ptarmiganpb.Event{}

	for _, event := range events {
		if KVMatchesKeyRange(keyRange, event.Kv) {
			filtered = append(filtered, event)
		}
	}

	return filtered
}

// KVMatchesKeyRange returns true if the key range matches the kv
func KVMatchesKeyRange(keyRange ptarmiganpb.KeyRange, kv ptarmiganpb.KeyValue) bool {
	if keyRange.Key != nil && bytes.Compare(kv.Key, keyRange.Key) != 0 {
		return false
	}

	if keyRange.KeyStartsWith != nil && !bytes.HasPrefix(kv.Key, keyRange.KeyStartsWith) {
		return false
	}

	switch keyRange.KeyRangeMin.(type) {
	case *ptarmiganpb.KeyRange_KeyGt:
		if keyRange.GetKeyGt() != nil && bytes.Compare(kv.Key, keyRange.GetKeyGt()) <= 0 {
			return false
		}
	case *ptarmiganpb.KeyRange_KeyGte:
		if keyRange.GetKeyGte() != nil && bytes.Compare(kv.Key, keyRange.GetKeyGte()) < 0 {
			return false
		}
	}

	switch keyRange.KeyRangeMax.(type) {
	case *ptarmiganpb.KeyRange_KeyLt:
		if keyRange.GetKeyLt() != nil && bytes.Compare(kv.Key, keyRange.GetKeyLt()) >= 0 {
			return false
		}
	case *ptarmiganpb.KeyRange_KeyLte:
		if keyRange.GetKeyLte() != nil && bytes.Compare(kv.Key, keyRange.GetKeyLte()) > 0 {
			return false
		}
	}

	return true
}

// KVMatchesSelection returns true if the selection matches the kv
func KVMatchesSelection(selection []ptarmiganpb.KVPredicate, kv ptarmiganpb.KeyValue) bool {
	for _, predicate := range selection {
		if !KVMatchesPredicate(predicate, kv) {
			return false
		}
	}

	return true
}

// KVMatchesPredicate returns true if the predicate matches the kv
func KVMatchesPredicate(predicate ptarmiganpb.KVPredicate, kv ptarmiganpb.KeyValue) bool {
	switch predicate.Field {
	case ptarmiganpb.VERSION:
		return kvMatchesNumPredicate(kv.Version, predicate.Value, predicate.Operator)
	case ptarmiganpb.CREATE:
		return kvMatchesNumPredicate(kv.CreateRevision, predicate.Value, predicate.Operator)
	case ptarmiganpb.MOD:
		return kvMatchesNumPredicate(kv.ModRevision, predicate.Value, predicate.Operator)
	case ptarmiganpb.LEASE:
		return kvMatchesNumPredicate(kv.Lease, predicate.Value, predicate.Operator)
	case ptarmiganpb.VALUE:
		return kvMatchesBytePredicate(kv.Value, predicate.Value, predicate.Operator)
	case ptarmiganpb.KEY:
		return kvMatchesBytePredicate(kv.Key, predicate.Value, predicate.Operator)
	}

	return true
}

func kvMatchesNumPredicate(a int64, b interface{}, operator ptarmiganpb.KVPredicate_Operator) bool {
	bInt, ok := b.(*ptarmiganpb.KVPredicate_Int64)

	if !ok {
		return false
	}

	switch operator {
	case ptarmiganpb.EQUAL:
		return a == bInt.Int64
	case ptarmiganpb.NOT_EQUAL:
		return a != bInt.Int64
	case ptarmiganpb.GT:
		return a > bInt.Int64
	case ptarmiganpb.GTE:
		return a >= bInt.Int64
	case ptarmiganpb.LT:
		return a < bInt.Int64
	case ptarmiganpb.LTE:
		return a <= bInt.Int64
	}

	return false
}

func kvMatchesBytePredicate(a []byte, b interface{}, operator ptarmiganpb.KVPredicate_Operator) bool {
	bBytes, ok := b.(*ptarmiganpb.KVPredicate_Bytes)

	if !ok {
		return false
	}

	cmp := bytes.Compare(a, bBytes.Bytes)

	switch operator {
	case ptarmiganpb.EQUAL:
		return cmp == 0
	case ptarmiganpb.NOT_EQUAL:
		return cmp == 0
	case ptarmiganpb.GT:
		return bBytes.Bytes == nil || cmp > 0
	case ptarmiganpb.GTE:
		return bBytes.Bytes == nil || cmp >= 0
	case ptarmiganpb.LT:
		return bBytes.Bytes == nil || cmp < 0
	case ptarmiganpb.LTE:
		return bBytes.Bytes == nil || cmp <= 0
	case ptarmiganpb.STARTS_WITH:
		// Must not be exactly equal.
		return bytes.HasPrefix(a, bBytes.Bytes) && cmp != 0
	}

	return false
}

func compareBytes(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

func compareInt64s(a, b interface{}) int {
	if a.(int64) < b.(int64) {
		return -1
	} else if a.(int64) > b.(int64) {
		return 1
	}

	return 0
}

func query(request ptarmiganpb.KVQueryRequest, state *ReplicaStoreModel) (ptarmiganpb.KVQueryResponse, bool) {
	var response ptarmiganpb.KVQueryResponse
	response.Kvs = []ptarmiganpb.KeyValue{}
	var revision RevisionModel

	if len(state.revisions) == 0 && request.Revision == mvcc.RevisionNewest {
		return ptarmiganpb.KVQueryResponse{Kvs: []ptarmiganpb.KeyValue{}}, true
	}

	revision, i := state.revision(request.Revision)

	if i == -1 {
		return ptarmiganpb.KVQueryResponse{}, false
	}

	kvs := revision.AllKvs().selection(request.Selection)

	if request.IncludeCount {
		response.Count = int64(len(kvs))
	}

	var cmp func(i, j int) int

	switch request.SortTarget {
	case ptarmiganpb.VERSION:
		cmp = func(i, j int) int {
			return compareInt64s(kvs[i].Version, kvs[j].Version)
		}
	case ptarmiganpb.CREATE:
		cmp = func(i, j int) int {
			return compareInt64s(kvs[i].CreateRevision, kvs[j].CreateRevision)
		}
	case ptarmiganpb.MOD:
		cmp = func(i, j int) int {
			return compareInt64s(kvs[i].ModRevision, kvs[j].ModRevision)
		}
	case ptarmiganpb.LEASE:
		cmp = func(i, j int) int {
			return compareInt64s(kvs[i].Lease, kvs[j].Lease)
		}
	case ptarmiganpb.VALUE:
		cmp = func(i, j int) int {
			return compareBytes(kvs[i].Value, kvs[j].Value)
		}
	}

	if cmp != nil {
		oldCmp := cmp
		cmp = func(i, j int) int {
			c := oldCmp(i, j)

			if c == 0 {
				return compareBytes(kvs[i].Key, kvs[j].Key)
			}

			return c
		}

		sort.Slice(kvs, func(i, j int) bool { return cmp(i, j) < 0 })
	}

	if request.SortOrder == ptarmiganpb.DESC {
		kvs = reverse(kvs)
	}

	kvs = after(request.After, kvs, request.SortTarget, request.SortOrder)

	if request.Limit > 0 && int64(len(kvs)) > request.Limit {
		kvs = kvs[:request.Limit]
		response.More = true
	}

	for _, kv := range kvs {
		kvCopy := kv

		if request.ExcludeValues {
			kvCopy.Value = nil
		}

		response.Kvs = append(response.Kvs, kvCopy)
	}

	if len(response.Kvs) > 0 {
		response.After = Cursor(response.Kvs[len(response.Kvs)-1], request.SortTarget)
	}

	return response, true
}

// Cursor generates a page cursor based on the kv and sort target
func Cursor(kv ptarmiganpb.KeyValue, sortTarget ptarmiganpb.Field) string {
	var cursorBytes []byte

	switch sortTarget {
	case ptarmiganpb.VERSION:
		cursorBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(cursorBytes, uint64(kv.Version))
	case ptarmiganpb.CREATE:
		cursorBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(cursorBytes, uint64(kv.CreateRevision))
	case ptarmiganpb.MOD:
		cursorBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(cursorBytes, uint64(kv.ModRevision))
	case ptarmiganpb.LEASE:
		cursorBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(cursorBytes, uint64(kv.Lease))
	case ptarmiganpb.VALUE:
		cursorBytes = kv.Value
	default:
		cursorBytes = kv.Key
	}

	return base64.StdEncoding.EncodeToString(cursorBytes)
}

// after truncates the kv list based on the page cursor and sort parameters
func after(encodedAfter string, kvs kvList, sortTarget ptarmiganpb.Field, sortOrder ptarmiganpb.KVQueryRequest_SortOrder) kvList {
	if encodedAfter == "" {
		return kvs
	}

	afterBytes, err := base64.StdEncoding.DecodeString(encodedAfter)

	if err != nil {
		return kvs
	}

	switch sortTarget {
	case ptarmiganpb.VERSION:
		fallthrough
	case ptarmiganpb.CREATE:
		fallthrough
	case ptarmiganpb.LEASE:
		fallthrough
	case ptarmiganpb.MOD:
		if len(afterBytes) != 8 {
			return kvs
		}

		afterInt := int64(binary.BigEndian.Uint64(afterBytes))

		for i, kv := range kvs {
			var kvInt int64

			switch sortTarget {
			case ptarmiganpb.VERSION:
				kvInt = kv.Version
			case ptarmiganpb.CREATE:
				kvInt = kv.CreateRevision
			case ptarmiganpb.LEASE:
				kvInt = kv.Lease
			case ptarmiganpb.MOD:
				kvInt = kv.ModRevision
			}

			if sortOrder == ptarmiganpb.DESC {
				if kvInt < afterInt {
					return kvs[i:]
				}
			} else {
				if kvInt > afterInt {
					return kvs[i:]
				}
			}
		}
	case ptarmiganpb.VALUE:
		for i, kv := range kvs {
			if sortOrder == ptarmiganpb.DESC {
				if bytes.Compare(kv.Value, afterBytes) < 0 {
					return kvs[i:]
				}
			} else {
				if bytes.Compare(kv.Value, afterBytes) > 0 {
					return kvs[i:]
				}
			}
		}
	default:
		for i, kv := range kvs {
			if sortOrder == ptarmiganpb.DESC {
				if bytes.Compare(kv.Key, afterBytes) < 0 {
					return kvs[i:]
				}
			} else {
				if bytes.Compare(kv.Key, afterBytes) > 0 {
					return kvs[i:]
				}
			}
		}
	}

	return kvList{}
}

func reverse(kvs kvList) kvList {
	reversed := kvList{}

	for i := len(kvs) - 1; i >= 0; i-- {
		reversed = append(reversed, kvs[i])
	}

	return reversed
}
