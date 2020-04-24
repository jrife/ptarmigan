package kv

import (
	"io"
)

type PluginOptions map[string]interface{}

// Plugin represents a kv storage plugin
type Plugin interface {
	// Init initializes the storage plugin
	Init(options PluginOptions) error
	// Name returns the name of the storage plugin
	Name() string
	// NewStore returns an instance of the plugin store
	NewStore(options PluginOptions) (Store, error)
	// NewTempStore returns an instance of the plugin store
	// initialized with some sane defaults. It is meant for
	// tests that need an initialized instance of the plugin's
	// store without knowing how to initialize it
	NewTempStore() (Store, error)
}

// Store represents an instance of a kv store
// Stores looks like a directory structure where
// each directory can contain files (keys) or other
// directories (buckets). A store must support
// serializable transactions.
type Store interface {
	// Close closes the store
	Close() error
	// Delete closes the store then deletes any
	// files associated with it
	Delete() error
	SubStore
}

// SubStore represents a handle to the store
// that is namespaced to some bucket. In keeping
// with the file system analogy, creating a SubStore
// from a parent store is like changing directory: all
// operations started from a SubStore are namespaced to
// that bucket. Both Bucket and SubStores act like references
// to something that may or may exist. You can create a SubStore
// object referring to a bucket does not exist. Likewise you can
// create a Bucket handle to a bucket that doesn't exist. In
// either case, reads and write operations will return an error
// if the current namespace is non-existent.
type SubStore interface {
	// Begin starts a transaction for the store namespaced
	// to the bucket associated with this SubStore. The
	// transaction will see its root as the current bucket.
	// writable should be true for read-write transactions and
	// false for read-only transactions.
	Begin(writable bool) (Transaction, error)
	// Snapshot takes a consistent snapshot of the bucket
	// associated with this SubStore. If the bucket that
	// this SubStore is namespaced to does not exist then
	// Snapshot will return an error.
	Snapshot() (io.ReadCloser, error)
	// ApplySnapshot applies a snapshot to the bucket
	// associated with this SubStore. Application of
	// a snapshot must preseve the consistency guarantees
	// of the store. In other words, applying a snapshot
	// is just a read-write transaction that completely
	// replaces the contents of the current bucket with
	// the snapshot. Atomicity and isolation guarantees
	// should be preserved. If the bucket that that this
	// SubStore is namespaced to does not exist then
	// ApplySnapshot will return an error.
	ApplySnapshot(io.Reader) error
	// Namespace creates a SubStore that is namespaced
	// to a child bucket of the current namespace.
	// It should return a SubStore regardless of whether
	// or not the bucket actually exists.
	Namespace(bucket []byte) SubStore
}

// Transaction represents a transaction used to interact
// with a kv store. A single transaction is not goroutine-safe
// In other words the caller must guarantee that at most one
// goroutine at a time interacts with a transaction.
type Transaction interface {
	// Root returns the root bucket for this transaction
	// If this transaction was created using the Begin()
	// function of a SubStore this will be the SubStore's
	// bucket. If it was the result of Namespace() being
	// called on another transaction it will be namespaced
	// to that bucket. This function returns nil if the bucket
	// does not exist.
	Root() Bucket
	// Namespace returns a handle to this transaction that
	// is namespaced to a new bucket. It is as if the
	// transaction had been created from a SubStore namespaced
	// to the specifeid bucket.
	Namespace(bucket []byte) Transaction
	// OnCommit sets up a handler to be invoked after this
	// transaction commits. Naturally this is not called
	// for read-only transactions. OnCommit should only
	// be called after the transaction has committed and
	// should not prevent the transaction from releasing
	// any locks required by other pending read-write
	// transactions.
	OnCommit(func())
	// Commit commits the transaction.
	Commit() error
	// Rollback rolls back the transaction.
	Rollback() error
}

// Bucket represents a handle to a bucket
type Bucket interface {
	// Bucket returns a handle to a child bucket
	// if it exists. It returns nil otherwise
	Bucket(name []byte) Bucket
	// Create creates this bucket. If the bucket
	// already exists this should have no effect.
	Create() error
	// Purge deletes this bucket. If this bucket doesn't
	// exist it just returns nil, having no effect.
	Purge() error
	// Return true if this bucket exists
	Exists() bool
	// Cursor returns a cursor that can be used
	// to navigate the current bucket.
	Cursor() Cursor
	// ForEachBucket invokes a function for each child
	// bucket in the current bucket. If the current bucket
	// does not exist then ForEachBucket must return an
	// error. If any instance of the function returns an
	// error ForEachBucket must cease iteration and return
	// that error itself.
	ForEachBucket(fn func(name []byte, bucket Bucket) error) error
	// ForEach invokes a function for each key contained
	// in the current bucket. IF the current bucket does not
	// exist then ForEach must return an error. If any instance of
	// the function returns an error ForEach must cease iteration
	// and return that error itself.
	ForEach(fn func(key []byte, value []byte) error) error
	// Delete deletes a key from the bucket. If the key
	// does not exist this must simply return nil and have
	// no effect. If the bucket does not exist then this
	// must return an error and have no effect.
	Delete(key []byte) error
	// Get reads a key from the bucket. If the bucket does
	// not exist then this must return an error.
	Get(key []byte) ([]byte, error)
	// NextSequence returns an autoincrementing
	// integer for this bucket. It can be used
	// for ID generation. If the bucket does not
	// exist this must return an error.
	NextSequence() (uint64, error)
	// Put puts a key in the bucket. If the bucket
	// does not exist then this must return an error.
	Put(key []byte, value []byte) error
	// Empty empties the bucket of all contents including
	// keys and child buckets. If the bucket does not
	// exist then this must return an error and have
	// no effect.
	Empty() error
}

type Cursor interface {
	Delete() error
	First() (key []byte, value []byte)
	Last() (key []byte, value []byte)
	Next() (key []byte, value []byte)
	Prev() (key []byte, value []byte)
	Seek(seek []byte) (key []byte, value []byte)
	Error() error
}
