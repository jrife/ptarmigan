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

func (kvs kvList) selection(selection *ptarmiganpb.KVSelection) kvList {
	if selection == nil {
		return kvs
	}

	sel := []ptarmiganpb.KeyValue{}

	for _, kv := range kvs {
		if KVMatchesSelection(selection, kv) {
			sel = append(sel, kv)
		}
	}

	return sel
}

func (kvs kvList) filter(predicate *ptarmiganpb.KVPredicate) kvList {
	if predicate == nil {
		return kvs
	}

	filtered := []ptarmiganpb.KeyValue{}

	for _, kv := range kvs {
		if KVMatchesPredicate(predicate, kv) {
			filtered = append(filtered, kv)
		}
	}

	return filtered
}

type eventList []ptarmiganpb.Event

func (events eventList) selection(selection *ptarmiganpb.KVSelection) eventList {
	if selection == nil {
		return events
	}

	sel := []ptarmiganpb.Event{}

	for _, event := range events {
		if KVMatchesSelection(selection, *event.Kv) {
			sel = append(sel, event)
		}
	}

	return sel
}

func KVMatchesSelection(selection *ptarmiganpb.KVSelection, kv ptarmiganpb.KeyValue) bool {
	if selection == nil {
		return true
	}

	if selection.CreateRevisionStart != nil {
		switch selection.CreateRevisionStart.(type) {
		case *ptarmiganpb.KVSelection_CreateRevisionGte:
			if kv.CreateRevision < selection.GetCreateRevisionGte() {
				return false
			}
		case *ptarmiganpb.KVSelection_CreateRevisionGt:
			if kv.CreateRevision <= selection.GetCreateRevisionGt() {
				return false
			}
		}
	}

	if selection.CreateRevisionEnd != nil {
		switch selection.CreateRevisionEnd.(type) {
		case *ptarmiganpb.KVSelection_CreateRevisionLte:
			if kv.CreateRevision > selection.GetCreateRevisionLte() {
				return false
			}
		case *ptarmiganpb.KVSelection_CreateRevisionLt:
			if kv.CreateRevision >= selection.GetCreateRevisionLt() {
				return false
			}
		}
	}

	if selection.ModRevisionStart != nil {
		switch selection.ModRevisionStart.(type) {
		case *ptarmiganpb.KVSelection_ModRevisionGte:
			if kv.ModRevision < selection.GetModRevisionGte() {
				return false
			}
		case *ptarmiganpb.KVSelection_ModRevisionGt:
			if kv.ModRevision <= selection.GetModRevisionGt() {
				return false
			}
		}
	}

	if selection.ModRevisionEnd != nil {
		switch selection.ModRevisionEnd.(type) {
		case *ptarmiganpb.KVSelection_ModRevisionLte:
			if kv.ModRevision > selection.GetModRevisionLte() {
				return false
			}
		case *ptarmiganpb.KVSelection_ModRevisionLt:
			if kv.ModRevision >= selection.GetModRevisionLt() {
				return false
			}
		}
	}

	if selection.Key != nil && bytes.Compare(kv.Key, selection.Key) != 0 {
		return false
	}

	if selection.KeyStartsWith != nil && !bytes.HasPrefix(kv.Key, selection.KeyStartsWith) {
		return false
	}

	switch selection.KeyRangeMin.(type) {
	case *ptarmiganpb.KVSelection_KeyGt:
		if selection.GetKeyGt() != nil && bytes.Compare(kv.Key, selection.GetKeyGt()) <= 0 {
			return false
		}
	case *ptarmiganpb.KVSelection_KeyGte:
		if selection.GetKeyGte() != nil && bytes.Compare(kv.Key, selection.GetKeyGte()) < 0 {
			return false
		}
	}

	switch selection.KeyRangeMax.(type) {
	case *ptarmiganpb.KVSelection_KeyLt:
		if selection.GetKeyLt() != nil && bytes.Compare(kv.Key, selection.GetKeyLt()) >= 0 {
			return false
		}
	case *ptarmiganpb.KVSelection_KeyLte:
		if selection.GetKeyLte() != nil && bytes.Compare(kv.Key, selection.GetKeyLte()) > 0 {
			return false
		}
	}

	if selection.GetLease() != 0 && selection.GetLease() != kv.Lease {
		return false
	}

	return true
}

func KVMatchesPredicate(predicate *ptarmiganpb.KVPredicate, kv ptarmiganpb.KeyValue) bool {
	if predicate == nil {
		return true
	}

	var numTarget int64
	var numCmp int64

	switch predicate.TargetUnion.(type) {
	case *ptarmiganpb.KVPredicate_CreateRevision:
		// Ignore malformed predicates
		if predicate.Target != ptarmiganpb.KVPredicate_CREATE {
			return false
		}
		numTarget = kv.CreateRevision
		numCmp = predicate.GetCreateRevision()
	case *ptarmiganpb.KVPredicate_Lease:
		// Ignore malformed predicates
		if predicate.Target != ptarmiganpb.KVPredicate_LEASE {
			return false
		}
		numTarget = kv.Lease
		numCmp = predicate.GetLease()
	case *ptarmiganpb.KVPredicate_Version:
		// Ignore malformed predicates
		if predicate.Target != ptarmiganpb.KVPredicate_VERSION {
			return false
		}
		numTarget = kv.Version
		numCmp = predicate.GetVersion()
	case *ptarmiganpb.KVPredicate_ModRevision:
		// Ignore malformed predicates
		if predicate.Target != ptarmiganpb.KVPredicate_MOD {
			return false
		}
		numTarget = kv.ModRevision
		numCmp = predicate.GetModRevision()
	case *ptarmiganpb.KVPredicate_Value:
		// Ignore malformed predicates
		if predicate.Target != ptarmiganpb.KVPredicate_VALUE {
			return false
		}

		cmp := bytes.Compare(kv.Value, predicate.GetValue())

		switch predicate.Comparison {
		case ptarmiganpb.KVPredicate_EQUAL:
			return cmp == 0
		case ptarmiganpb.KVPredicate_NOT_EQUAL:
			return cmp != 0
		case ptarmiganpb.KVPredicate_GREATER:
			return cmp > 0
		case ptarmiganpb.KVPredicate_LESS:
			return cmp < 0
		}

		return false
	default:
		return false
	}

	switch predicate.Comparison {
	case ptarmiganpb.KVPredicate_EQUAL:
		return numTarget == numCmp
	case ptarmiganpb.KVPredicate_NOT_EQUAL:
		return numTarget != numCmp
	case ptarmiganpb.KVPredicate_GREATER:
		return numTarget > numCmp
	case ptarmiganpb.KVPredicate_LESS:
		return numTarget < numCmp
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
	response.Kvs = []*ptarmiganpb.KeyValue{}
	var revision RevisionModel

	if len(state.revisions) == 0 && request.Revision == mvcc.RevisionNewest {
		return ptarmiganpb.KVQueryResponse{Kvs: []*ptarmiganpb.KeyValue{}}, true
	}

	revision, i := state.revision(request.Revision)

	if i == -1 {
		return ptarmiganpb.KVQueryResponse{}, false
	}

	kvs := revision.AllKvs().selection(request.Selection)

	if request.IncludeCount {
		response.Count = int64(len(kvs))
	}

	var cmp func(i, j int) bool

	switch request.SortTarget {
	case ptarmiganpb.KVQueryRequest_VERSION:
		cmp = func(i, j int) bool {
			return compareInt64s(kvs[i].Version, kvs[j].Version) < 0
		}
	case ptarmiganpb.KVQueryRequest_CREATE:
		cmp = func(i, j int) bool {
			return compareInt64s(kvs[i].CreateRevision, kvs[j].CreateRevision) < 0
		}
	case ptarmiganpb.KVQueryRequest_MOD:
		cmp = func(i, j int) bool {
			return compareInt64s(kvs[i].ModRevision, kvs[j].ModRevision) < 0
		}
	case ptarmiganpb.KVQueryRequest_VALUE:
		cmp = func(i, j int) bool {
			return compareBytes(kvs[i].Value, kvs[j].Value) < 0
		}
	}

	if cmp != nil {
		sort.Slice(kvs, cmp)
	}

	if request.SortOrder == ptarmiganpb.KVQueryRequest_DESC {
		kvs = reverse(kvs)
	}

	kvs = After(request.After, kvs, request.SortTarget, request.SortOrder)

	if request.Limit > 0 && int64(len(kvs)) > request.Limit {
		kvs = kvs[:request.Limit]
		response.More = true
	}

	for _, kv := range kvs {
		kvCopy := kv

		if request.ExcludeValues {
			kvCopy.Value = nil
		}

		response.Kvs = append(response.Kvs, &kvCopy)
	}

	if len(response.Kvs) > 0 {
		response.After = Cursor(*response.Kvs[len(response.Kvs)-1], request.SortTarget)
	}

	return response, true
}

func Cursor(kv ptarmiganpb.KeyValue, sortTarget ptarmiganpb.KVQueryRequest_SortTarget) string {
	var cursorBytes []byte

	switch sortTarget {
	case ptarmiganpb.KVQueryRequest_VERSION:
		cursorBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(cursorBytes, uint64(kv.Version))
	case ptarmiganpb.KVQueryRequest_CREATE:
		cursorBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(cursorBytes, uint64(kv.CreateRevision))
	case ptarmiganpb.KVQueryRequest_MOD:
		cursorBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(cursorBytes, uint64(kv.ModRevision))
	case ptarmiganpb.KVQueryRequest_VALUE:
		cursorBytes = kv.Value
	default:
		cursorBytes = kv.Key
	}

	return base64.StdEncoding.EncodeToString(cursorBytes)
}

func After(encodedAfter string, kvs kvList, sortTarget ptarmiganpb.KVQueryRequest_SortTarget, sortOrder ptarmiganpb.KVQueryRequest_SortOrder) kvList {
	if encodedAfter == "" {
		return kvs
	}

	afterBytes, err := base64.StdEncoding.DecodeString(encodedAfter)

	if err != nil {
		return kvs
	}

	switch sortTarget {
	case ptarmiganpb.KVQueryRequest_VERSION:
		fallthrough
	case ptarmiganpb.KVQueryRequest_CREATE:
		fallthrough
	case ptarmiganpb.KVQueryRequest_MOD:
		if len(afterBytes) != 8 {
			return kvs
		}

		afterInt := int64(binary.BigEndian.Uint64(afterBytes))

		for i, kv := range kvs {
			var kvInt int64

			switch sortTarget {
			case ptarmiganpb.KVQueryRequest_VERSION:
				kvInt = kv.Version
			case ptarmiganpb.KVQueryRequest_CREATE:
				kvInt = kv.CreateRevision
			case ptarmiganpb.KVQueryRequest_MOD:
				kvInt = kv.ModRevision
			}

			if sortOrder == ptarmiganpb.KVQueryRequest_DESC {
				if kvInt < afterInt {
					return kvs[i:]
				}
			} else {
				if kvInt > afterInt {
					return kvs[i:]
				}
			}
		}
	case ptarmiganpb.KVQueryRequest_VALUE:
		for i, kv := range kvs {
			if sortOrder == ptarmiganpb.KVQueryRequest_DESC {
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
			if sortOrder == ptarmiganpb.KVQueryRequest_DESC {
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
