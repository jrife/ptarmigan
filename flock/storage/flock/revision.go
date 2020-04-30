package flock

import "github.com/jrife/ptarmigan/storage/mvcc"

var _ View = (*revision)(nil)
var _ Revision = (*revision)(nil)

// revision implements Revision
type revision struct {
	view
	transaction mvcc.Transaction
}

func (revision *revision) mvccRevision() mvcc.Revision {
	rev, ok := revision.view.view.(mvcc.Revision)

	if !ok {
		panic("misuse of revision type. it requires that view is actually an instance of mvcc.Revision")
	}

	return rev
}

// Put implements Revision.Put
func (revision *revision) Put(key []byte, value []byte) error {
	return nil
}

// Delete implements Revision.Delete
func (revision *revision) Delete(key []byte) error {
	return nil
}

// Commit implements Revision.Commit
func (revision *revision) Commit() error {
	return nil
}

// Abort implements Revision.Rollback
func (revision *revision) Rollback() error {
	return nil
}
