package composite

import (
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/keys/composite"
	"github.com/jrife/ptarmigan/storage/snapshot"
)

// Partition is like kv.Partition except Begin
// returns composite.Transaction instead of kv.Transaction
type Partition interface {
	Name() []byte
	Create(metadata []byte) error
	Delete() error
	Begin(writable bool) (Transaction, error)
	snapshot.Source
	snapshot.Acceptor
}

// Transaction is a transaction for a partition. It must only be
// used by one goroutine at a time.
type Transaction interface {
	Map
	// Metadata returns the metadata for this partition
	Metadata() ([]byte, error)
	// SetMetadata sets the metadata for this partition
	SetMetadata(metadata []byte) error
	// Commit commits the transaction
	Commit() error
	// Rollback rolls back the transaction
	Rollback() error
}

// MapUpdater is an interface for updating a sorted
// key-value map whose keys consist of a sequence of byte slices
type MapUpdater interface {
	Put(key composite.Key, value []byte) error
	Delete(key composite.Key) error
}

// MapReader is an interface for reading a sorted key-value
// map whose keys consist of a sequence of byte slices
type MapReader interface {
	Get(key composite.Key) ([]byte, error)
	Keys(keys composite.Range, order kv.SortOrder) (Iterator, error)
}

// Map combines CompositeMapReader and CompositeMapUpdater
type Map interface {
	MapReader
	MapUpdater
}

// Iterator iterates over a set of composite keys. It must only be
// used by one goroutine at a time. Consumers should not
// attempt to use an iterator once its parent transaction
// has been rolled back. Behavior is undefined in this case.
// The transaction must not mutate the store when the iterator
// is in use. This may cause inconsistent behavior.
type Iterator interface {
	Next() bool
	Key() composite.Key
	Value() []byte
	Error() error
}
