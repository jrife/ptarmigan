package flock

import (
	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

var _ View = (*view)(nil)

// view implements View
type view struct {
	view mvcc.View
}

// Query implements View.Query
func (view *view) Query(query flockpb.KVQueryRequest) (flockpb.KVQueryResponse, error) {
	return flockpb.KVQueryResponse{}, nil
}

// Get implements View.Get
func (view *view) Get(key []byte) (flockpb.KeyValue, error) {
	return flockpb.KeyValue{}, nil
}

// Revision implements View.Revision
func (view *view) Revision() int64 {
	return view.view.Revision()
}

// Changes implements View.Changes
func (view *view) Changes(start []byte, limit int) ([]flockpb.Event, error) {
	return nil, nil
}

func (view *view) Close() error {
	return view.view.Close()
}
