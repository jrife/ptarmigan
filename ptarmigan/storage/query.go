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
		results = stream.Pipeline(results, after(query), sort(query, int(limit)), stream.Limit(int(limit)))
	}

	for results.Next() {
		kv := results.Value().(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue)
		kv.Key = results.Value().(kv_marshaled.KV).Key()

		logger.Debug("next KeyValue", zap.Any("value", kv))

		if query.Limit > 0 && int64(len(response.Kvs)) == query.Limit {
			response.More = true

			continue
		}

		response.Kvs = append(response.Kvs, &kv)
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
		response.After = base64.StdEncoding.EncodeToString(response.Kvs[len(response.Kvs)-1].Key)
	}

	return response, nil
}

// Changes implements View.Changes
func changes(view mvcc.View, start []byte, limit int, includePrev bool) ([]ptarmiganpb.Event, error) {
	diffsIter, err := view.Changes(keys.All().Gt(start), includePrev)

	if err != nil {
		return nil, fmt.Errorf("could not list changes: %s", err)
	}

	diffs, err := mvcc.Diffs(diffsIter, limit)

	if err != nil {
		return nil, fmt.Errorf("could not list changes: %s", err)
	}

	events := make([]ptarmiganpb.Event, len(diffs))

	for i, diff := range diffs {
		event := ptarmiganpb.Event{Type: ptarmiganpb.Event_PUT}

		if diff[1] == nil {
			event.Type = ptarmiganpb.Event_DELETE
		}

		if diff[1] != nil {
			kv, err := unmarshalKV(diff[1])

			if err != nil {
				return nil, fmt.Errorf("could not unmarshal value %#v: %s", diff[1], err)
			}

			k := kv.(ptarmiganpb.KeyValue)
			event.Kv = &k
		}

		if includePrev && diff[2] != nil {
			kv, err := unmarshalKV(diff[2])

			if err != nil {
				return nil, fmt.Errorf("could not unmarshal value %#v: %s", diff[1], err)
			}

			k := kv.(ptarmiganpb.KeyValue)
			event.PrevKv = &k
		}

		events[i] = event
	}

	return events, nil
}

func makeKvs(limit int64) []*ptarmiganpb.KeyValue {
	// Try to pick the best initial capacity for the
	// result
	capacity := int64(defaultBufferInitialCapacity)

	if limit > 0 {
		if limit <= maxBufferInitialCapacity {
			capacity = limit
		}

		capacity = maxBufferInitialCapacity
	}

	return make([]*ptarmiganpb.KeyValue, 0, capacity)
}

func sortOrder(order ptarmiganpb.KVQueryRequest_SortOrder) kv.SortOrder {
	if order == ptarmiganpb.KVQueryRequest_DESC {
		return kv.SortOrderDesc
	}

	return kv.SortOrderAsc
}

func keyRange(query ptarmiganpb.KVQueryRequest) keys.Range {
	return refineRange(selectionRange(query.Selection), query)
}

// return the minimum required key range required to retrieve all keys matching the selection
// excluding any considerations for paging options such as "after" or "limit" in the query.
func selectionRange(selection *ptarmiganpb.KVSelection) keys.Range {
	keyRange := keys.All()

	if selection == nil {
		return keyRange
	}

	// short circuits any range specifier
	if len(selection.GetKey()) != 0 {
		return keyRange.Eq(selection.GetKey())
	}

	switch selection.KeyRangeMin.(type) {
	case *ptarmiganpb.KVSelection_KeyGte:
		keyRange = keyRange.Gte(selection.GetKeyGte())
	case *ptarmiganpb.KVSelection_KeyGt:
		keyRange = keyRange.Gt(selection.GetKeyGt())
	}

	switch selection.KeyRangeMax.(type) {
	case *ptarmiganpb.KVSelection_KeyLte:
		keyRange = keyRange.Lte(selection.GetKeyLte())
	case *ptarmiganpb.KVSelection_KeyLt:
		keyRange = keyRange.Lt(selection.GetKeyLt())
	}

	if len(selection.GetKeyStartsWith()) != 0 {
		keyRange = keyRange.Prefix(selection.GetKeyStartsWith())
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

	if query.SortTarget != ptarmiganpb.KVQueryRequest_KEY {
		// Key order is not correlated with sort order for query. Must search entire
		// range and filter.
		return keyRange
	}

	if query.After != "" {
		switch query.SortOrder {
		case ptarmiganpb.KVQueryRequest_DESC:
			keyRange = keyRange.Lt([]byte(query.After))
		case ptarmiganpb.KVQueryRequest_ASC:
			fallthrough
		default:
			keyRange = keyRange.Gt([]byte(query.After))
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
	switch query.SortTarget {
	case ptarmiganpb.KVQueryRequest_VERSION:
		return stream.Filter(func(a interface{}) bool {
			if len(a.([]byte)) != 8 {
				return true
			}

			v := int64(binary.BigEndian.Uint64(a.([]byte)[:]))

			return a.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).Version > v
		})
	case ptarmiganpb.KVQueryRequest_CREATE:
		return stream.Filter(func(a interface{}) bool {
			if len(a.([]byte)) != 8 {
				return true
			}

			v := int64(binary.BigEndian.Uint64(a.([]byte)[:]))

			return a.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).CreateRevision > v
		})
	case ptarmiganpb.KVQueryRequest_MOD:
		return stream.Filter(func(a interface{}) bool {
			if len(a.([]byte)) != 8 {
				return true
			}

			v := int64(binary.BigEndian.Uint64(a.([]byte)[:]))

			return a.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).ModRevision > v
		})
	case ptarmiganpb.KVQueryRequest_VALUE:
		return stream.Filter(func(a interface{}) bool {
			return bytes.Compare(a.(kv_marshaled.KV).Value().(ptarmiganpb.KeyValue).Value, []byte(query.After)) > 0
		})
	default:
		return nil
	}
}

func selection(selection *ptarmiganpb.KVSelection) stream.Processor {
	if selection == nil {
		return nil
	}

	return stream.Filter(func(v interface{}) bool {
		kv := v.(ptarmiganpb.KeyValue)

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

		if selection.GetLease() != 0 && selection.GetLease() != kv.Lease {
			return false
		}

		return true
	})
}

func sort(query ptarmiganpb.KVQueryRequest, limit int) stream.Processor {
	var compare func(a interface{}, b interface{}) int

	switch query.SortTarget {
	case ptarmiganpb.KVQueryRequest_VERSION:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(ptarmiganpb.KeyValue)
			kvB := b.(ptarmiganpb.KeyValue)

			if kvA.Version < kvB.Version {
				return -1
			} else if kvA.Version > kvB.Version {
				return 1
			}

			return 0
		}
	case ptarmiganpb.KVQueryRequest_CREATE:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(ptarmiganpb.KeyValue)
			kvB := b.(ptarmiganpb.KeyValue)

			if kvA.CreateRevision < kvB.CreateRevision {
				return -1
			} else if kvA.CreateRevision > kvB.CreateRevision {
				return 1
			}

			return 0
		}
	case ptarmiganpb.KVQueryRequest_MOD:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(ptarmiganpb.KeyValue)
			kvB := b.(ptarmiganpb.KeyValue)

			if kvA.ModRevision < kvB.ModRevision {
				return -1
			} else if kvA.ModRevision > kvB.ModRevision {
				return 1
			}

			return 0
		}
	case ptarmiganpb.KVQueryRequest_VALUE:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(ptarmiganpb.KeyValue)
			kvB := b.(ptarmiganpb.KeyValue)

			return bytes.Compare(kvA.Value, kvB.Value)
		}
	default:
		return nil
	}

	if query.SortOrder == ptarmiganpb.KVQueryRequest_DESC {
		compare = func(a interface{}, b interface{}) int {
			return -1 * compare(a, b)
		}
	}

	return stream.Sort(compare, limit)
}
