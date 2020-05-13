package flock

import (
	"fmt"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/kv/keys"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

var _ View = (*view)(nil)

// view implements View
type view struct {
	view mvcc.View
}

// Get implements View.Get
func (view *view) Get(key []byte) (flockpb.KeyValue, error) {
	kv, err := kvMapReader(view.view).Get(key)

	if err != nil {
		return flockpb.KeyValue{}, fmt.Errorf("could not get key %#v: %s", key, err)
	}

	if kv == nil {
		return flockpb.KeyValue{}, ErrNotFound
	}

	return kv.(flockpb.KeyValue), nil
}

// Revision implements View.Revision
func (view *view) Revision() int64 {
	return view.view.Revision()
}

// Changes implements View.Changes
func (view *view) Changes(start []byte, limit int, includePrev bool) ([]flockpb.Event, error) {
	diffsIter, err := view.view.Changes(keys.All().Gt(start), includePrev)

	if err != nil {
		return nil, fmt.Errorf("could not list changes: %s", err)
	}

	diffs, err := mvcc.Diffs(diffsIter, limit)

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
			kv, err := unmarshalKV(diff[1])

			if err != nil {
				return nil, fmt.Errorf("could not unmarshal value %#v: %s", diff[1], err)
			}

			k := kv.(flockpb.KeyValue)
			event.Kv = &k
		}

		if includePrev && diff[2] != nil {
			kv, err := unmarshalKV(diff[2])

			if err != nil {
				return nil, fmt.Errorf("could not unmarshal value %#v: %s", diff[1], err)
			}

			k := kv.(flockpb.KeyValue)
			event.PrevKv = &k
		}

		events[i] = event
	}

	return events, nil
}

func (view *view) Close() error {
	return view.view.Close()
}
