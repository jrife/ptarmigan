package mvcc

import (
	"errors"
	"fmt"

	"github.com/jrife/ptarmigan/storage/kv"
)

var (
	// ErrClosed indicates that the store was closed
	ErrClosed = errors.New("store was closed")
	// ErrCompacted is returned when an operation cannot be completed
	// because it needs to read data that was compacted away.
	ErrCompacted = errors.New("store was compacted")
	// ErrRevisionTooHigh is returned when an operation tries to access
	// a revision number that is higher than the newest committed revision.
	ErrRevisionTooHigh = errors.New("revision number is higher than the newest committed revision")
	// ErrNoSuchPartition is returned when a function tries to access a partition
	// that does not exist.
	ErrNoSuchPartition = errors.New("partition does not exist")
	// ErrNoRevisions is returned when a consumer requests a view of either the oldest
	// or newest revision, but the partition is empty having had no revisions
	// written to it yet.
	ErrNoRevisions = errors.New("partition has no revisions")
	// ErrTooManyRevisions is returned when a transaction tries to create more than
	// one revision
	ErrTooManyRevisions = errors.New("transaction already created a new revision")
	// ErrReadOnly is returned when a read-only transaction attempts an update operation
	ErrReadOnly = errors.New("transaction is read-only")
)

func wrapError(wrap string, err error) error {
	switch err {
	case kv.ErrClosed:
		return ErrClosed
	case kv.ErrNoSuchPartition:
		return ErrNoSuchPartition
	case ErrClosed:
		fallthrough
	case nil:
		return err
	}

	return fmt.Errorf("%s: %s", wrap, err)
}
