package flock

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/mvcc"
	"github.com/jrife/ptarmigan/utils/stream"
)

const (
	maxBufferInitialCapacity     = 1000
	defaultBufferInitialCapacity = 100
)

// Query implements View.Query
func (view *view) Query(query flockpb.KVQueryRequest) (flockpb.KVQueryResponse, error) {
	min, max := keyRange(query)
	iter, err := view.view.KeysIterator(kvsKey(min), kvsKey(max), sortOrder(query.SortOrder))

	if err != nil {
		return flockpb.KVQueryResponse{}, fmt.Errorf("could not create keys iterator: %s", err)
	}

	var response flockpb.KVQueryResponse = flockpb.KVQueryResponse{Kvs: makeKvs(query.Limit)}
	var results stream.Stream = stream.Pipeline(mvcc.StreamMarshaled(iter), selection(query.Selection))
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

	if query.IncludeCount {
		counted = stream.Pipeline(results, count(&response.Count))
		results = stream.Pipeline(counted, after(query), sort(query, int(limit)), stream.Limit(int(limit)))
	} else {
		results = stream.Pipeline(results, after(query), sort(query, int(limit)), stream.Limit(int(limit)))
	}

	for results.Next() {
		if query.Limit > 0 && int64(len(response.Kvs)) == query.Limit {
			response.More = true

			continue
		}

		kv := results.Value().(flockpb.KeyValue)
		response.Kvs = append(response.Kvs, &kv)
	}

	if counted != nil {
		// Drain counted so we count the rest of the keys matching the
		// selection. This is necessary because the results stream may
		// stop after limit is hit depending on the limit setting.
		for counted.Next() {
		}
	}

	return response, nil
}

func makeKvs(limit int64) []*flockpb.KeyValue {
	// Try to pick the best initial capacity for the
	// result
	capacity := int64(defaultBufferInitialCapacity)

	if limit > 0 {
		if limit <= maxBufferInitialCapacity {
			capacity = limit
		}

		capacity = maxBufferInitialCapacity
	}

	return make([]*flockpb.KeyValue, 0, capacity)
}

func sortOrder(order flockpb.KVQueryRequest_SortOrder) mvcc.SortOrder {
	if order == flockpb.KVQueryRequest_DESC {
		return mvcc.SortOrderDesc
	}

	return mvcc.SortOrderAsc
}

func keyRange(query flockpb.KVQueryRequest) ([]byte, []byte) {
	min, max := selectionRange(query.Selection)
	min, max = refineRange(min, max, query)

	return min, max
}

// return the minimum required key range required to retrieve all keys matching the selection
// excluding any considerations for paging options such as "after" or "limit" in the query.
func selectionRange(selection *flockpb.KVSelection) ([]byte, []byte) {
	if selection == nil {
		return nil, nil
	}

	// short circuits any range specifier
	if len(selection.GetKey()) != 0 {
		return kv.Gte(selection.GetKey()), kv.Lte(selection.GetKey())
	}

	var min []byte
	var max []byte

	switch selection.KeyRangeMin.(type) {
	case *flockpb.KVSelection_KeyGte:
		min = kv.Gte(selection.GetKeyGte())
	case *flockpb.KVSelection_KeyGt:
		min = kv.Gt(selection.GetKeyGt())
	}

	switch selection.KeyRangeMax.(type) {
	case *flockpb.KVSelection_KeyLte:
		max = kv.Lte(selection.GetKeyLte())
	case *flockpb.KVSelection_KeyLt:
		max = kv.Lt(selection.GetKeyLt())
	}

	if len(selection.GetKeyStartsWith()) != 0 {
		start := kv.PrefixRangeStart(selection.GetKeyStartsWith())
		end := kv.PrefixRangeEnd(selection.GetKeyStartsWith())

		// final min must satisfy both key range restrictions
		// and prefix restriction. If the prefix range start
		// key comes after the minimum range key then the minimum
		// range key should be the prefix range start
		if min == nil || bytes.Compare(start, min) > 0 {
			min = start
		}

		// final max must satisfy both key range restrictions
		// and prefix restriction. If the prefix range end
		// key comes before the maximum range key then the maximum
		// range key should be the prefix range end
		if max == nil || bytes.Compare(end, max) < 0 {
			max = end
		}
	}

	return min, max
}

// refineRange shrinks the search space if possible by making min or max more restrictive if
// the query allows.
func refineRange(min, max []byte, query flockpb.KVQueryRequest) ([]byte, []byte) {
	if query.IncludeCount {
		// Must count all keys matching the selection every time. Cannot refine range
		return min, max
	}

	if query.SortTarget != flockpb.KVQueryRequest_KEY {
		// Key order is not correlated with sort order for query. Must search entire
		// range and filter.
		return min, max
	}

	if query.After != "" {
		switch query.SortOrder {
		case flockpb.KVQueryRequest_DESC:
			if max == nil || bytes.Compare([]byte(query.After), max) < 0 {
				max = []byte(query.After)
			}
		case flockpb.KVQueryRequest_ASC:
			fallthrough
		default:
			after := kv.Gt([]byte(query.After))
			if min == nil || bytes.Compare([]byte(after), min) > 0 {
				min = after
			}
		}
	}

	return min, max
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

func after(query flockpb.KVQueryRequest) stream.Processor {
	switch query.SortTarget {
	case flockpb.KVQueryRequest_VERSION:
		return stream.Filter(func(a interface{}) bool {
			if len(a.([]byte)) != 8 {
				return true
			}

			v := int64(binary.BigEndian.Uint64(a.([]byte)[:]))

			return a.(mvcc.UnmarshaledKV)[1].(flockpb.KeyValue).Version > v
		})
	case flockpb.KVQueryRequest_CREATE:
		return stream.Filter(func(a interface{}) bool {
			if len(a.([]byte)) != 8 {
				return true
			}

			v := int64(binary.BigEndian.Uint64(a.([]byte)[:]))

			return a.(mvcc.UnmarshaledKV)[1].(flockpb.KeyValue).CreateRevision > v
		})
	case flockpb.KVQueryRequest_MOD:
		return stream.Filter(func(a interface{}) bool {
			if len(a.([]byte)) != 8 {
				return true
			}

			v := int64(binary.BigEndian.Uint64(a.([]byte)[:]))

			return a.(mvcc.UnmarshaledKV)[1].(flockpb.KeyValue).ModRevision > v
		})
	case flockpb.KVQueryRequest_VALUE:
		return stream.Filter(func(a interface{}) bool {
			return bytes.Compare(a.(mvcc.UnmarshaledKV)[1].(flockpb.KeyValue).Value, []byte(query.After)) > 0
		})
	default:
		return nil
	}
}

func selection(selection *flockpb.KVSelection) stream.Processor {
	if selection == nil {
		return nil
	}

	return stream.Filter(func(v interface{}) bool {
		kv := v.(flockpb.KeyValue)

		switch selection.CreateRevisionStart.(type) {
		case *flockpb.KVSelection_CreateRevisionGte:
			if kv.CreateRevision < selection.GetCreateRevisionGte() {
				return false
			}
		case *flockpb.KVSelection_CreateRevisionGt:
			if kv.CreateRevision <= selection.GetCreateRevisionGt() {
				return false
			}
		}

		switch selection.CreateRevisionEnd.(type) {
		case *flockpb.KVSelection_CreateRevisionLte:
			if kv.CreateRevision > selection.GetCreateRevisionLte() {
				return false
			}
		case *flockpb.KVSelection_CreateRevisionLt:
			if kv.CreateRevision >= selection.GetCreateRevisionLt() {
				return false
			}
		}

		switch selection.ModRevisionStart.(type) {
		case *flockpb.KVSelection_ModRevisionGte:
			if kv.ModRevision < selection.GetModRevisionGte() {
				return false
			}
		case *flockpb.KVSelection_ModRevisionGt:
			if kv.ModRevision <= selection.GetModRevisionGt() {
				return false
			}
		}

		switch selection.ModRevisionEnd.(type) {
		case *flockpb.KVSelection_ModRevisionLte:
			if kv.ModRevision > selection.GetModRevisionLte() {
				return false
			}
		case *flockpb.KVSelection_ModRevisionLt:
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

func sort(query flockpb.KVQueryRequest, limit int) stream.Processor {
	var compare func(a interface{}, b interface{}) int

	switch query.SortTarget {
	case flockpb.KVQueryRequest_VERSION:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(flockpb.KeyValue)
			kvB := b.(flockpb.KeyValue)

			if kvA.Version < kvB.Version {
				return -1
			} else if kvA.Version > kvB.Version {
				return 1
			}

			return 0
		}
	case flockpb.KVQueryRequest_CREATE:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(flockpb.KeyValue)
			kvB := b.(flockpb.KeyValue)

			if kvA.CreateRevision < kvB.CreateRevision {
				return -1
			} else if kvA.CreateRevision > kvB.CreateRevision {
				return 1
			}

			return 0
		}
	case flockpb.KVQueryRequest_MOD:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(flockpb.KeyValue)
			kvB := b.(flockpb.KeyValue)

			if kvA.ModRevision < kvB.ModRevision {
				return -1
			} else if kvA.ModRevision > kvB.ModRevision {
				return 1
			}

			return 0
		}
	case flockpb.KVQueryRequest_VALUE:
		compare = func(a interface{}, b interface{}) int {
			kvA := a.(flockpb.KeyValue)
			kvB := b.(flockpb.KeyValue)

			return bytes.Compare(kvA.Value, kvB.Value)
		}
	default:
		return nil
	}

	if query.SortOrder == flockpb.KVQueryRequest_DESC {
		compare = func(a interface{}, b interface{}) int {
			return -1 * compare(a, b)
		}
	}

	return stream.Sort(compare, limit)
}
