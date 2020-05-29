package ptarmigan

import (
	"fmt"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/storage/mvcc"
)

var _ View = (*view)(nil)

// view implements View
type view struct {
	view        mvcc.View
	transaction mvcc.Transaction
}

// Get implements View.Get
func (view *view) Get(key []byte) (ptarmiganpb.KeyValue, error) {
	kv, err := kvMapReader(view.view).Get(key)

	if err != nil {
		return ptarmiganpb.KeyValue{}, fmt.Errorf("could not get key %#v: %s", key, err)
	}

	if kv == nil {
		return ptarmiganpb.KeyValue{}, ErrNotFound
	}

	return kv.(ptarmiganpb.KeyValue), nil
}

// Revision implements View.Revision
func (view *view) Revision() int64 {
	return view.view.Revision()
}

// Changes implements View.Changes
func (view *view) Changes(start []byte, limit int, includePrev bool) ([]ptarmiganpb.Event, error) {
	diffsIter, err := view.view.Changes(keys.All().Gt(start), includePrev)

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

func (view *view) Close() error {
	return view.transaction.Rollback()
}
