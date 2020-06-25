package storage

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
	kv_marshaled "github.com/jrife/flock/storage/kv/marshaled"
	"github.com/jrife/flock/storage/mvcc"
	"go.uber.org/zap"

	"github.com/jrife/flock/utils/stream"
)

const (
	maxBufferInitialCapacity     = 1000
	defaultBufferInitialCapacity = 100
)

func copy(b []byte) []byte {
	return append([]byte(nil), b...)
}

func query(logger *zap.Logger, view mvcc.View, query ptarmiganpb.KVQueryRequest) (ptarmiganpb.KVQueryResponse, error) {
	kr := keyRange(query)
	logger.Debug("key range chosen", zap.Binary("min", kr.Min), zap.Binary("max", kr.Max))
	iter, err := kvMapReader(view).Keys(kr, sortOrder(query.SortOrder))

	if err != nil {
		return ptarmiganpb.KVQueryResponse{}, fmt.Errorf("could not create keys iterator: %s", err)
	}

	var response ptarmiganpb.KVQueryResponse = ptarmiganpb.KVQueryResponse{Kvs: makeKvs(query.Limit)}
	var results stream.Stream = stream.Pipeline(kv_marshaled.Stream(iter), selection(query.Selection))
	var counted stream.Stream
	var limit int64

	if query.Limit+1 > 1 {
		// Request one additional for More property.
		// If this condition isn't true then query.Limit
		// is either 0, negative, or the max int value.
		// In all those cases there is no limit and we will
		// retrieve the entire matching key set so more
		// should always be false
		limit = query.Limit + 1
	}

	logger.Debug("limit chosen", zap.Int64("limit", limit))

	if query.IncludeCount {
		counted = stream.Pipeline(results, count(&response.Count))
		results = stream.Pipeline(counted, after(query), sort(query, int(limit)), stream.Limit(int(limit)))
	} else {
		results = stream.Pipeline(
			results,
			after(query),
			sort(query, int(limit)),
			stream.Limit(int(limit)),
		)
	}

	for results.Next() {
		kv := results.Value().(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
		kv.Key = copy(results.Value().(kv_marshaled.KV).Key())

		if query.ExcludeValues {
			kv.Value = nil
		}

		logger.Debug("next KeyValue", zap.Any("value", kv))

		if query.Limit > 0 && int64(len(response.Kvs)) == query.Limit {
			response.More = true

			continue
		}

		response.Kvs = append(response.Kvs, kv)
	}

	if results.Error() != nil {
		return ptarmiganpb.KVQueryResponse{}, fmt.Errorf("iteration error: %s", err)
	}

	if counted != nil {
		logger.Debug("drain")

		// Drain counted so we count the rest of the keys matching the
		// selection. This is necessary because the results stream may
		// stop after limit is hit depending on the limit setting.
		for counted.Next() {
		}

		if counted.Error() != nil {
			return ptarmiganpb.KVQueryResponse{}, fmt.Errorf("counted iteration error: %s", err)
		}
	}

	if len(response.Kvs) > 0 {
		response.After = cursor(response.Kvs[len(response.Kvs)-1], query.SortTarget)
	}

	return response, nil
}

func cursor(kv ptarmiganpb.KeyValue, sortTarget ptarmiganpb.Field) string {
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

// Changes implements View.Changes
func changes(events []ptarmiganpb.Event, view mvcc.View, watchRequest ptarmiganpb.KVWatchRequest, limit int) ([]ptarmiganpb.Event, error) {
	kr := keyRangeWatch(watchRequest)
	diffsIter, err := view.Changes(kr)

	if err != nil {
		return nil, fmt.Errorf("could not list changes: %s", err)
	}

	prev, err := view.Prev()

	if err != nil && err != mvcc.ErrCompacted {
		return nil, fmt.Errorf("could not retrieve view for previous revision: %s", err)
	}

	for diffsIter.Next() && (limit <= 0 || len(events) < limit) {
		event := ptarmiganpb.Event{Type: ptarmiganpb.PUT}

		if diffsIter.IsDelete() {
			if watchRequest.NoDelete {
				continue
			}

			event.Type = ptarmiganpb.DELETE
			event.Kv = ptarmiganpb.KeyValue{
				Key:         copy(diffsIter.Key()),
				ModRevision: view.Revision(),
			}

			if prev != nil && watchRequest.PrevKv {
				rawKV, err := kvMapReader(prev).Get(diffsIter.Key())

				if err != nil {
					return nil, fmt.Errorf("could not get key %#v from previous revision: %s", diffsIter.Key(), err)
				}

				if rawKV == nil {
					return nil, fmt.Errorf("consistency violation: revision %d contains a delete event for key %#v but the previous revision does not contain that key", view.Revision(), diffsIter.Key())
				}

				prevKV := rawKV.(ptarmiganpb.KeyValue)
				prevKV.Key = event.Kv.Key
				event.PrevKv = &prevKV
			}
		} else if diffsIter.IsPut() {
			if watchRequest.NoPut {
				continue
			}

			kv, err := unmarshalKV(diffsIter.Value())

			if err != nil {
				return nil, fmt.Errorf("could not unmarshal value %#v: %s", diffsIter.Value(), err)
			}

			k := kv.(ptarmiganpb.KeyValue)

			k.Key = copy(diffsIter.Key())
			event.Kv = k

			if prev != nil && watchRequest.PrevKv {
				rawKV, err := kvMapReader(prev).Get(diffsIter.Key())

				if err != nil {
					return nil, fmt.Errorf("could not get key %#v from previous revision: %s", diffsIter.Key(), err)
				}

				if rawKV != nil {
					prevKV := rawKV.(ptarmiganpb.KeyValue)
					prevKV.Key = k.Key
					event.PrevKv = &prevKV
				}
			}
		}

		events = append(events, event)
	}

	if diffsIter.Error() != nil {
		return nil, fmt.Errorf("iteration error: %s", diffsIter.Error())
	}

	return events, nil
}

func makeKvs(limit int64) []ptarmiganpb.KeyValue {
	// Try to pick the best initial capacity for the
	// result
	capacity := int64(defaultBufferInitialCapacity)

	if limit > 0 {
		if limit <= maxBufferInitialCapacity {
			capacity = limit
		}

		capacity = maxBufferInitialCapacity
	}

	return make([]ptarmiganpb.KeyValue, 0, capacity)
}

func sortOrder(order ptarmiganpb.KVQueryRequest_SortOrder) kv.SortOrder {
	if order == ptarmiganpb.DESC {
		return kv.SortOrderDesc
	}

	return kv.SortOrderAsc
}

func keyRange(query ptarmiganpb.KVQueryRequest) keys.Range {
	return refineRange(selectionRange(query.Selection), query)
}

func keyRangeWatch(watch ptarmiganpb.KVWatchRequest) keys.Range {
	keyRange := keys.All()

	// short circuits any range specifier
	if watch.Keys.Key != nil {
		keyRange = keyRange.Eq(watch.Keys.Key)
	}

	switch watch.Keys.KeyRangeMin.(type) {
	case *ptarmiganpb.KeyRange_KeyGte:
		keyRange = keyRange.Gte(watch.Keys.GetKeyGte())
	case *ptarmiganpb.KeyRange_KeyGt:
		keyRange = keyRange.Gt(watch.Keys.GetKeyGt())
	}

	switch watch.Keys.KeyRangeMax.(type) {
	case *ptarmiganpb.KeyRange_KeyLte:
		keyRange = keyRange.Lte(watch.Keys.GetKeyLte())
	case *ptarmiganpb.KeyRange_KeyLt:
		keyRange = keyRange.Lt(watch.Keys.GetKeyLt())
	}

	if len(watch.Keys.KeyStartsWith) != 0 {
		keyRange = keyRange.Prefix(watch.Keys.KeyStartsWith)
	}

	if watch.Start.Key != nil {
		keyRange = keyRange.Gt(watch.Start.Key)
	}

	return keyRange
}

// return the minimum required key range required to retrieve all keys matching the selection
// excluding any considerations for paging options such as "after" or "limit" in the query.
func selectionRange(selection []ptarmiganpb.KVPredicate) keys.Range {
	keyRange := keys.All()

	for _, predicate := range selection {
		if predicate.Field != ptarmiganpb.KEY {
			continue
		}

		keyTarget, ok := predicate.Value.(*ptarmiganpb.KVPredicate_Bytes)

		if !ok {
			// skip malformed predicates where value doesn't match
			continue
		}

		switch predicate.Operator {
		case ptarmiganpb.EQUAL:
			keyRange = keyRange.Eq(keyTarget.Bytes)
		case ptarmiganpb.NOT_EQUAL:
			// requires full scan
			return keys.All()
		case ptarmiganpb.GT:
			keyRange = keyRange.Gt(keyTarget.Bytes)
		case ptarmiganpb.GTE:
			keyRange = keyRange.Gte(keyTarget.Bytes)
		case ptarmiganpb.LT:
			keyRange = keyRange.Lt(keyTarget.Bytes)
		case ptarmiganpb.LTE:
			keyRange = keyRange.Lte(keyTarget.Bytes)
		case ptarmiganpb.STARTS_WITH:
			keyRange = keyRange.Prefix(keyTarget.Bytes)
		}
	}

	return keyRange
}

// refineRange shrinks the search space if possible by making min or max more restrictive if
// the query allows.
func refineRange(keyRange keys.Range, query ptarmiganpb.KVQueryRequest) keys.Range {
	if query.IncludeCount {
		// Must count all keys matching the selection every time. Cannot refine range
		return keyRange
	}

	switch query.SortTarget {
	case ptarmiganpb.VERSION:
		fallthrough
	case ptarmiganpb.CREATE:
		fallthrough
	case ptarmiganpb.MOD:
		fallthrough
	case ptarmiganpb.LEASE:
		fallthrough
	case ptarmiganpb.VALUE:
		return keyRange
	}

	if query.After != "" {
		a, err := base64.StdEncoding.DecodeString(query.After)

		if err == nil {
			switch query.SortOrder {
			case ptarmiganpb.DESC:
				keyRange = keyRange.Lt(a)
			case ptarmiganpb.ASC:
				fallthrough
			default:
				keyRange = keyRange.Gt(a)
			}
		}
	}

	return keyRange
}

func count(c *int64) stream.Processor {
	return func(s stream.Stream) stream.Stream {
		return &counter{s, c}
	}
}

type counter struct {
	stream.Stream
	c *int64
}

func (counter *counter) Next() bool {
	if !counter.Stream.Next() {
		return false
	}

	*(counter.c)++

	return true
}

func after(query ptarmiganpb.KVQueryRequest) stream.Processor {
	a, err := base64.StdEncoding.DecodeString(query.After)

	if err != nil {
		a = []byte{}
	}

	switch query.SortTarget {
	case ptarmiganpb.VERSION:
		return stream.Filter(func(rawKV interface{}) bool {
			if len(a) != 8 {
				return true
			}

			if query.SortOrder == ptarmiganpb.DESC {
				return rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).Version < int64(binary.BigEndian.Uint64(a))
			}

			return rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).Version > int64(binary.BigEndian.Uint64(a))
		})
	case ptarmiganpb.CREATE:
		return stream.Filter(func(rawKV interface{}) bool {
			if len(a) != 8 {
				return true
			}

			if query.SortOrder == ptarmiganpb.DESC {
				return rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).CreateRevision < int64(binary.BigEndian.Uint64(a))
			}

			return rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).CreateRevision > int64(binary.BigEndian.Uint64(a))
		})
	case ptarmiganpb.MOD:
		return stream.Filter(func(rawKV interface{}) bool {
			if len(a) != 8 {
				return true
			}

			if query.SortOrder == ptarmiganpb.DESC {
				return rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).ModRevision < int64(binary.BigEndian.Uint64(a))
			}

			return rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).ModRevision > int64(binary.BigEndian.Uint64(a))
		})
	case ptarmiganpb.LEASE:
		return stream.Filter(func(rawKV interface{}) bool {
			if len(a) != 8 {
				return true
			}

			if query.SortOrder == ptarmiganpb.DESC {
				return rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).Lease < int64(binary.BigEndian.Uint64(a))
			}

			return rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).Lease > int64(binary.BigEndian.Uint64(a))
		})
	case ptarmiganpb.VALUE:
		return stream.Filter(func(rawKV interface{}) bool {
			if len(a) == 0 {
				return true
			}

			if query.SortOrder == ptarmiganpb.DESC {
				return bytes.Compare(rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).Value, a) < 0
			}

			return bytes.Compare(rawKV.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).Value, a) > 0
		})
	default:
		if !query.IncludeCount {
			return nil
		}

		// Must count all keys matching the selection every time. Must enforce cursor here instead of letting KV driver do it.
		return stream.Filter(func(rawKV interface{}) bool {
			if len(a) == 0 {
				return true
			}

			if query.SortOrder == ptarmiganpb.DESC {
				return bytes.Compare(rawKV.(kv_marshaled.KV).Key(), a) < 0
			}

			return bytes.Compare(rawKV.(kv_marshaled.KV).Key(), a) > 0
		})
	}
}

func selection(selection []ptarmiganpb.KVPredicate) stream.Processor {
	if len(selection) == 0 {
		return nil
	}

	return stream.Filter(func(v interface{}) bool {
		kv := v.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
		kv.Key = v.(kv_marshaled.KV).Key()
		return kvMatchesSelection(kv, selection)
	})
}

func kvMatchesSelection(kv ptarmiganpb.KeyValue, selection []ptarmiganpb.KVPredicate) bool {
	for _, predicate := range selection {
		if !kvMatchesPredicate(kv, predicate, true) {
			return false
		}
	}

	return true
}

func kvMatchesPredicate(kv ptarmiganpb.KeyValue, predicate ptarmiganpb.KVPredicate, ignoreKeyPredicates bool) bool {
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
		// Even if ignoreKeyPredicates is set,
		// we have to check key not equal predicates because such a predicate
		// requires a scan of the full key space.
		_, isBytes := predicate.Value.(*ptarmiganpb.KVPredicate_Bytes)

		if ignoreKeyPredicates && predicate.Operator != ptarmiganpb.NOT_EQUAL {
			return predicate.Operator >= ptarmiganpb.EQUAL && predicate.Operator <= ptarmiganpb.STARTS_WITH && isBytes
		}

		return kvMatchesBytePredicate(kv.Key, predicate.Value, predicate.Operator)
	}

	// If it's an unrecognized comparison, skip the predicate
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
		return bytes.HasPrefix(a, bBytes.Bytes) && cmp != 0
	}

	return false
}

func sort(query ptarmiganpb.KVQueryRequest, limit int) stream.Processor {
	var compare func(a interface{}, b interface{}) int

	switch query.SortTarget {
	case ptarmiganpb.VERSION:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
			kvB := b.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)

			if kvA.Version < kvB.Version {
				return -1
			} else if kvA.Version > kvB.Version {
				return 1
			}

			return bytes.Compare(a.(kv_marshaled.KV).Key(), b.(kv_marshaled.KV).Key())
		}
	case ptarmiganpb.CREATE:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
			kvB := b.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)

			if kvA.CreateRevision < kvB.CreateRevision {
				return -1
			} else if kvA.CreateRevision > kvB.CreateRevision {
				return 1
			}

			return bytes.Compare(a.(kv_marshaled.KV).Key(), b.(kv_marshaled.KV).Key())
		}
	case ptarmiganpb.MOD:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
			kvB := b.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)

			if kvA.ModRevision < kvB.ModRevision {
				return -1
			} else if kvA.ModRevision > kvB.ModRevision {
				return 1
			}

			return bytes.Compare(a.(kv_marshaled.KV).Key(), b.(kv_marshaled.KV).Key())
		}
	case ptarmiganpb.LEASE:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
			kvB := b.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)

			if kvA.Lease < kvB.Lease {
				return -1
			} else if kvA.Lease > kvB.Lease {
				return 1
			}

			return bytes.Compare(a.(kv_marshaled.KV).Key(), b.(kv_marshaled.KV).Key())
		}
	case ptarmiganpb.VALUE:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
			kvB := b.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)

			cmp := bytes.Compare(kvA.Value, kvB.Value)

			if cmp != 0 {
				return cmp
			}

			return bytes.Compare(a.(kv_marshaled.KV).Key(), b.(kv_marshaled.KV).Key())
		}
	default:
		return nil
	}

	if query.SortOrder == ptarmiganpb.DESC {
		oldCompare := compare
		compare = func(a interface{}, b interface{}) int {
			return -1 * oldCompare(a, b)
		}
	}

	return stream.Sort(compare, limit)
}
