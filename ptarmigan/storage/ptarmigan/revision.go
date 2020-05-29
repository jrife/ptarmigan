package ptarmigan

import (
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/storage/mvcc"
)

var _ View = (*revision)(nil)
var _ Revision = (*revision)(nil)

// revision implements Revision
type revision struct {
	view
	revision    mvcc.Revision
	transaction mvcc.Transaction
}

// Put implements Revision.Put
func (revision *revision) Put(req ptarmiganpb.KVPutRequest) (ptarmiganpb.KVPutResponse, error) {
	return ptarmiganpb.KVPutResponse{}, nil
}

// Delete implements Revision.Delete
func (revision *revision) Delete(req ptarmiganpb.KVDeleteRequest) (ptarmiganpb.KVDeleteResponse, error) {
	return ptarmiganpb.KVDeleteResponse{}, nil
}

// Commit implements Revision.Commit
func (revision *revision) Commit() error {
	return revision.transaction.Commit()
}

// Abort implements Revision.Rollback
func (revision *revision) Rollback() error {
	return revision.transaction.Rollback()
}
