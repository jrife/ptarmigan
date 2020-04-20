package mvcc

import "github.com/jrife/ptarmigan/flock/server/flockpb"

var _ IView = (*Revision)(nil)
var _ IRevision = (*Revision)(nil)

// Revision implements IView and IRevision. It lets
// you build a new revision and explore the state of
// a replica store at some revision.
type Revision struct {
}

// Query executes a query on this view (ignores revision in request)
func (revision *Revision) Query(query flockpb.KVQueryRequest) (flockpb.KVQueryResponse, error) {
	return flockpb.KVQueryResponse{}, nil
}

// Get reads a key at a revision. It returns ErrNotFound if the key
// does not exist at this revision.
func (revision *Revision) Get(key []byte) (flockpb.KeyValue, error) {
	return flockpb.KeyValue{}, nil
}

// Revision returns the revision index of this view
func (revision *Revision) Revision() int64 {
	return 0
}

// Changes returns a set of change events associated with this
// revision. Events are always sorted in ascending order by the
// byte order of their key. Returns events whose key is > start up
// to limit. In other words, start acts as a cursor and limit
// limits the size of the resulting events.
func (revision *Revision) Changes(start []byte, limit int) ([]flockpb.Event, error) {
	return nil, nil
}

// Put creates or updates a key.
func (revision *Revision) Put(key []byte, value []byte) error {
	return nil
}

// Delete deletes a key. If the key doesn't
// exist no error is returned.
func (revision *Revision) Delete(key []byte) error {
	return nil
}

// Commit commits this revision, making its
// effects visible.
func (revision *Revision) Commit() error {
	return nil
}

// Abort discards this revision. Its effects
// will not be visible to any observers.
func (revision *Revision) Abort() error {
	return nil
}
