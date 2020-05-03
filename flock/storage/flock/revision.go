package flock

import (
	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

var _ View = (*revision)(nil)
var _ Revision = (*revision)(nil)

// revision implements Revision
type revision struct {
	view
	revision    *mvcc.MarshalingRevision
	transaction mvcc.Transaction
}

// Put implements Revision.Put
func (revision *revision) Put(req flockpb.KVPutRequest) (flockpb.KVPutResponse, error) {
	return flockpb.KVPutResponse{}, nil
}

// Delete implements Revision.Delete
func (revision *revision) Delete(req flockpb.KVDeleteRequest) (flockpb.KVDeleteResponse, error) {
	return flockpb.KVDeleteResponse{}, nil
}

// Commit implements Revision.Commit
func (revision *revision) Commit() error {
	return revision.transaction.Commit()
}

// Abort implements Revision.Rollback
func (revision *revision) Rollback() error {
	return revision.transaction.Rollback()
}
