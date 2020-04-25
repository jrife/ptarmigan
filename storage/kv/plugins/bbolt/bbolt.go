package bbolt

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/utils/uuid"
	bolt "go.etcd.io/bbolt"
)

const (
	// DriverName describes the name for this driver
	DriverName = "bbolt"
)

var (
	storesBucket     = []byte{0}
	kvBucket         = []byte{0}
	partitionsBucket = []byte{0}
)

// Plugins returns a list of kv plugins
// implemented by this package.
func Plugins() []kv.Plugin {
	return []kv.Plugin{
		&Plugin{},
	}
}

var _ kv.Plugin = (*Plugin)(nil)

// Plugin implements kv.Plugin for bbolt
type Plugin struct {
}

// Init implements Plugin.Init
func (plugin *Plugin) Init(options kv.PluginOptions) error {
	return nil
}

// Name implements Plugin.Name
func (plugin *Plugin) Name() string {
	return DriverName
}

// NewRootStore implements Plugin.NewRootStore
func (plugin *Plugin) NewRootStore(options kv.PluginOptions) (kv.RootStore, error) {
	var config RootStoreConfig

	if path, ok := options["path"]; !ok {
		return nil, fmt.Errorf("\"path\" is required")
	} else if pathString, ok := path.(string); !ok {
		return nil, fmt.Errorf("\"path\" must be a string")
	} else {
		config.Path = pathString
	}

	store, err := NewRootStore(config)

	if err != nil {
		return nil, err
	}

	return store, nil
}

// NewTempRootStore implements Plugin.NewTempRootStore
func (plugin *Plugin) NewTempRootStore() (kv.RootStore, error) {
	return plugin.NewRootStore(kv.PluginOptions{
		"path": fmt.Sprintf("/tmp/bbolt-%s", uuid.MustUUID()),
	})
}

// RootStoreConfig contains configuration
// options for a bbolt root store
type RootStoreConfig struct {
	// Path is the path to the bbolt data directory
	Path string
}

var _ kv.RootStore = (*RootStore)(nil)

// RootStore implements kv.RootStore on top of a single
// bbolt store.
type RootStore struct {
	db *bolt.DB
}

// NewRootStore creates a new bbolt root store
func NewRootStore(config RootStoreConfig) (*RootStore, error) {
	db, err := bolt.Open(config.Path, 0666, nil)

	if err != nil {
		return nil, fmt.Errorf("could not open bbolt store at %s: %s", config.Path, err.Error())
	}

	if err := db.Update(func(txn *bolt.Tx) error {
		_, err := txn.CreateBucketIfNotExists(storesBucket)

		return err
	}); err != nil {
		db.Close()

		return nil, fmt.Errorf("could not ensure stores bucket exists: %s", err.Error())
	}

	return &RootStore{
		db: db,
	}, nil
}

// Delete implements RootStore.Delete
func (rootStore *RootStore) Delete() error {
	if err := rootStore.Close(); err != nil {
		return fmt.Errorf("could not close root store: %s", err.Error())
	}

	if err := os.RemoveAll(rootStore.db.Path()); err != nil {
		return fmt.Errorf("could not remove directory: %s", err.Error())
	}

	return nil
}

// Close implements RootStore.Close
func (rootStore *RootStore) Close() error {
	return rootStore.db.Close()
}

// Stores implements RootStore.Stores
func (rootStore *RootStore) Stores() ([][]byte, error) {
	stores := [][]byte{}

	if err := rootStore.db.View(func(txn *bolt.Tx) error {
		storesBucket := txn.Bucket(storesBucket)

		if storesBucket == nil {
			return fmt.Errorf("unexpected error: could not retrieve stores bucket")
		}

		return storesBucket.ForEach(func(name []byte, value []byte) error {
			stores = append(stores, name)

			return nil
		})
	}); err != nil {
		if err == bolt.ErrDatabaseNotOpen {
			err = kv.ErrClosed
		}

		return nil, err
	}

	return stores, nil
}

// Store implements RootStore.Store
func (rootStore *RootStore) Store(name []byte) kv.Store {
	return &Store{rootStore: rootStore, name: name}
}

var _ kv.Store = (*Store)(nil)

// Store implements kv.Store on top of a bbolt store
// bucket
type Store struct {
	rootStore *RootStore
	name      []byte
}

func (store *Store) bucket(txn *bolt.Tx) (*bolt.Bucket, error) {
	stores := txn.Bucket(storesBucket)

	if stores == nil {
		return nil, fmt.Errorf("unexpected error: could not retrieve stores bucket")
	}

	storeBucket := stores.Bucket(store.name)

	if storeBucket == nil {
		return nil, kv.ErrNoSuchStore
	}

	return storeBucket, nil
}

// Name implements Store.Name
func (store *Store) Name() []byte {
	return store.name
}

// Create implements Store.Create
func (store *Store) Create() error {
	return store.rootStore.db.Update(func(txn *bolt.Tx) error {
		stores := txn.Bucket(storesBucket)

		if stores == nil {
			return fmt.Errorf("unexpected error: could not retrieve stores bucket")
		}

		if bucket, err := stores.CreateBucketIfNotExists(store.name); err != nil {
			return fmt.Errorf("could not create bucket %s: %s", store.name, err.Error())
		} else if _, err := bucket.CreateBucketIfNotExists(partitionsBucket); err != nil {
			return fmt.Errorf("could not create kv bucket inside store bucket %s: %s", store.name, err.Error())
		}

		return nil
	})
}

// Delete implements Store.Delete
func (store *Store) Delete() error {
	return store.rootStore.db.Update(func(txn *bolt.Tx) error {
		stores := txn.Bucket(storesBucket)

		if stores == nil {
			return fmt.Errorf("unexpected error: could not retrieve stores bucket")
		}

		if err := stores.DeleteBucket(store.name); err != nil {
			if err == bolt.ErrBucketNotFound {
				return nil
			}

			return fmt.Errorf("could not delete bucket %s: %s", store.name, err.Error())
		}

		return nil
	})
}

// Partitions implements Store.Partitions
func (store *Store) Partitions(min, max []byte, limit int) ([][]byte, error) {
	partitions := [][]byte{}

	if err := store.rootStore.db.View(func(txn *bolt.Tx) error {
		storeBucket, err := store.bucket(txn)

		if err != nil {
			return err
		}

		partBucket := storeBucket.Bucket(partitionsBucket)

		if partBucket == nil {
			return fmt.Errorf("unexpected error: could not retrieve partitions bucket inside store bucket %s", store.name)
		}

		cursor := partBucket.Cursor()

		var nextPartition []byte

		if min == nil {
			nextPartition, _ = cursor.First()
		} else {
			nextPartition, _ = cursor.Seek(min)
		}

		for nextPartition != nil && (limit < 0 || len(partitions) < limit) && (max == nil || bytes.Compare(nextPartition, max) < 0) {
			partitions = append(partitions, nextPartition)
			nextPartition, _ = cursor.Next()
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return partitions, nil
}

// Partition implements Store.Partition
func (store *Store) Partition(name []byte) kv.Partition {
	return &Partition{store: store, name: name}
}

var _ kv.Partition = (*Partition)(nil)

// Partition implements kv.Partition on top of a bbolt store
// bucket
type Partition struct {
	store *Store
	name  []byte
}

func (partition *Partition) bucket(txn *bolt.Tx) (*bolt.Bucket, error) {
	storeBucket, err := partition.store.bucket(txn)

	if err != nil {
		return nil, err
	}

	partBucket := storeBucket.Bucket(partitionsBucket)

	if partBucket == nil {
		return nil, fmt.Errorf("unexpected error: could not retrieve partitions bucket inside store bucket %s", partition.store.name)
	}

	myBucket := partBucket.Bucket(partition.name)

	if myBucket == nil {
		return nil, kv.ErrNoSuchPartition
	}

	return myBucket, nil
}

// Name implements Partition.Name
func (partition *Partition) Name() []byte {
	return partition.name
}

// Create implemenets Partition.Create
func (partition *Partition) Create() error {
	return partition.store.rootStore.db.Update(func(txn *bolt.Tx) error {
		storeBucket, err := partition.store.bucket(txn)

		if err != nil {
			return err
		}

		partBucket := storeBucket.Bucket(partitionsBucket)

		if partBucket == nil {
			return fmt.Errorf("unexpected error: could not retrieve partitions bucket inside store bucket %s", partition.store.name)
		}

		_, err = partBucket.CreateBucketIfNotExists(partition.name)

		if err != nil {
			return fmt.Errorf("could not create partition %s inside store %s: %s", partition.name, partition.store.name, err.Error())
		}

		return nil
	})
}

// Delete implements Partition.Delete
func (partition *Partition) Delete() error {
	return partition.store.rootStore.db.Update(func(txn *bolt.Tx) error {
		storeBucket, err := partition.store.bucket(txn)

		if err != nil {
			return err
		}

		partBucket := storeBucket.Bucket(partitionsBucket)

		if partBucket == nil {
			return fmt.Errorf("unexpected error: could not retrieve partitions bucket inside store bucket %s", partition.store.name)
		}

		partitionBucket := partBucket.Bucket(partition.name)

		if partitionBucket == nil {
			return nil
		}

		if err := partBucket.DeleteBucket(partition.name); err != nil {
			return fmt.Errorf("could not delete partition bucket %s: %s", partition.name, err.Error())
		}

		return nil
	})
}

// Begin implements Partition.Begin
func (partition *Partition) Begin(writable bool) (kv.Transaction, error) {
	txn, err := partition.store.rootStore.db.Begin(writable)

	if err != nil {
		return nil, fmt.Errorf("could not begin bolt transaction: %s", err.Error())
	}

	partitionBucket, err := partition.bucket(txn)

	if err != nil {
		txn.Rollback()

		return nil, err
	}

	return &Transaction{txn: txn, bucket: partitionBucket}, nil
}

// Snapshot implements Partition.Snapshot
func (partition *Partition) Snapshot() (io.ReadCloser, error) {
	return nil, nil
}

// ApplySnapshot implements Partition.ApplySnapshot
func (partition *Partition) ApplySnapshot(io.Reader) error {
	return nil
}

var _ kv.Transaction = (*Transaction)(nil)

// Transaction implements kv.Transaction on top of
// a bbolt transaction.
type Transaction struct {
	txn    *bolt.Tx
	bucket *bolt.Bucket
}

// Put implements Transaction.Put
func (transaction *Transaction) Put(key, value []byte) error {
	return transaction.bucket.Put(key, value)
}

// Get implements Transaction.Get
func (transaction *Transaction) Get(key []byte) ([]byte, error) {
	return transaction.bucket.Get(key), nil
}

// Delete implements Transaction.Delete
func (transaction *Transaction) Delete(key []byte) error {
	return transaction.bucket.Delete(key)
}

// Keys implements Transaction.Keys
func (transaction *Transaction) Keys(min, max []byte, order kv.SortOrder) (kv.Iterator, error) {
	iter := &Iterator{}
	cursor := transaction.bucket.Cursor()
	advance := func() ([]byte, []byte) {
		if order == kv.SortOrderDesc {
			return cursor.Prev()
		}

		return cursor.Next()
	}
	end := func(key []byte) bool {
		if order == kv.SortOrderDesc {
			return min != nil && bytes.Compare(key, min) < 0
		}

		return max != nil && bytes.Compare(key, max) >= 0
	}

	iter.next = func() ([]byte, []byte) {
		iter.next = func() ([]byte, []byte) {
			k, v := advance()

			if k == nil || end(k) {
				return nil, nil
			}

			return k, v
		}

		if order == kv.SortOrderDesc {
			if max == nil {
				return cursor.Last()
			}

			cursor.Seek(max)
			return cursor.Prev()
		}

		if min == nil {
			return cursor.First()
		}

		return cursor.Seek(min)
	}

	return iter, nil
}

// Commit implements Transaction.Commit
func (transaction *Transaction) Commit() error {
	return transaction.txn.Commit()
}

// Rollback implements Transaction.Rollback
func (transaction *Transaction) Rollback() error {
	return transaction.txn.Rollback()
}

// Iterator implements kv.Iterator on top of a
// bbolt cursor
type Iterator struct {
	next  func() ([]byte, []byte)
	key   []byte
	value []byte
	done  bool
}

// Next implements Iterator.Next
func (iterator *Iterator) Next() bool {
	if iterator.done {
		return false
	}

	k, v := iterator.next()

	if k == nil {
		iterator.done = true

		return false
	}

	iterator.key = k
	iterator.value = v

	return true
}

// Key implements Iterator.Key
func (iterator *Iterator) Key() []byte {
	return iterator.key
}

// Value implements Iterator.Value
func (iterator *Iterator) Value() []byte {
	return iterator.value
}

// Error implements Iterator.Error
func (iterator *Iterator) Error() error {
	return nil
}
