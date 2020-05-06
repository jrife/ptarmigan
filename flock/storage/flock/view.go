package flock

import (
	"fmt"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

var _ View = (*view)(nil)

// view implements View
type view struct {
	view *mvcc.MarshalingView
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
