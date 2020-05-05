package flock

import (
	"bytes"
	"fmt"

	"github.com/jrife/ptarmigan/utils/stream"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

func sortOrder(order flockpb.KVQueryRequest_SortOrder) mvcc.SortOrder {
	if order == flockpb.KVQueryRequest_DESC {
		return mvcc.SortOrderDesc
	}

	return mvcc.SortOrderAsc
}

func filter(selection *flockpb.KVSelection) func(interface{}) bool {
	return func(v interface{}) bool {
		if selection == nil {
			return true
		}

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
	}
}

var _ View = (*view)(nil)

// view implements View
type view struct {
	view *mvcc.MarshalingView
}

// Query implements View.Query
func (view *view) Query(query flockpb.KVQueryRequest) (flockpb.KVQueryResponse, error) {
	// Parameters: SortTarget, SortOrder, Selection, After, Limit, ExcludeValues, IncludeCount
	// SortTarget = KEY     -> natural order, no need for full scan + windowing unless IncludeCount is set
	//   selection keys -> count
	//                  -> filter -> sort(limit)
	// SortTarget = VERSION -> requires full scan + windowing
	// SortTarget = CREATE  -> requires full scan + windowing
	// SortTarget = MOD     -> requires full scan + windowing
	// SortTarget = VALUE   -> requires full scan + windowing
	processors := []stream.Processor{}

	switch query.SortTarget {
	case flockpb.KVQueryRequest_KEY:
		// keys follow natural order/sort order of query
		// full scan only depends on include count
		// after cursor is the key
		iter, err := view.selectionIterator(query.Selection, query.SortOrder)

		if err != nil {
			return flockpb.KVQueryResponse{}, fmt.Errorf("could not create selection iterator: %s", err)
		}

		stream.Pipeline(mvcc.StreamMarshaled(iter), stream.Filter(filter(query.Selection)))
	case flockpb.KVQueryRequest_VERSION:
	case flockpb.KVQueryRequest_CREATE:
	case flockpb.KVQueryRequest_MOD:
	case flockpb.KVQueryRequest_VALUE:
		// full scan always
		// after cursor depends on sort target
		// requires windowing
		iter, err := view.selectionIterator(query.Selection, flockpb.KVQueryRequest_ASC)

		if err != nil {
			return flockpb.KVQueryResponse{}, fmt.Errorf("could not create selection iterator: %s", err)
		}

		stream.Pipeline(mvcc.StreamMarshaled(iter), stream.Filter(filter(query.Selection)), stream.Sort(nil, int(query.Limit)))
	}

	return flockpb.KVQueryResponse{}, nil
}

func (view *view) selectionIterator(selection *flockpb.KVSelection, order flockpb.KVQueryRequest_SortOrder) (mvcc.UnmarshaledIterator, error) {
	min, max := view.keysRange(selection)
	iter, err := view.view.KeysIterator(min, max, sortOrder(order))

	if err != nil {
		return nil, fmt.Errorf("could not create keys iterator: %s", err)
	}

	return iter, nil
}

func (view *view) keysRange(selection *flockpb.KVSelection) (mvcc.Marshalable, mvcc.Marshalable) {
	if selection == nil {
		return nil, nil
	}

	// short circuits any range specifier
	if len(selection.GetKey()) != 0 {
		return gte(kvsKey(selection.GetKey())), lte(kvsKey(selection.GetKey()))
	}

	var min kvsKey
	var max kvsKey

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

// Get implements View.Get
func (view *view) Get(key []byte) (flockpb.KeyValue, error) {
	kvs, err := view.view.Keys(gte(kvsKey(key)), lte(kvsKey(key)), -1, mvcc.SortOrderAsc)

	if err != nil {
		return flockpb.KeyValue{}, fmt.Errorf("could not get key %#v: %s", key, err)
	}

	if len(kvs) == 0 {
		return flockpb.KeyValue{}, ErrKeyNotFound
	}

	return keyValues(kvs)[0], nil
}

// Revision implements View.Revision
func (view *view) Revision() int64 {
	return view.view.Revision()
}

// Changes implements View.Changes
func (view *view) Changes(start []byte, limit int, includePrev bool) ([]flockpb.Event, error) {
	diffs, err := view.view.Changes(gt(kvsKey(start)), nil, limit, includePrev)

	if err != nil {
		return nil, fmt.Errorf("could not list changes: %s", err)
	}

	events := make([]flockpb.Event, len(diffs))

	for i, diff := range diffs {
		event := flockpb.Event{Type: flockpb.Event_PUT}

		if diff[1] == nil {
			event.Type = flockpb.Event_DELETE
		}

		if diff[1] != nil {
			kv := diff[1].(flockpb.KeyValue)
			event.Kv = &kv
		}

		if includePrev && diff[2] != nil {
			kv := diff[2].(flockpb.KeyValue)
			event.PrevKv = &kv
		}

		events[i] = event
	}

	return events, nil
}

func (view *view) Close() error {
	return view.view.Close()
}
