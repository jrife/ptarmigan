package bbolt

import (
	"bytes"
	"fmt"
	"os"

	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/utils/uuid"
	bolt "go.etcd.io/bbolt"
)

const (
	// DriverName describes the name for this driver
	DriverName = "bbolt"
)

var (
	storesBucket     = []byte{0}
	kvBucket         = []byte{0}
	metaBucket       = []byte{1}
	metaKey          = []byte{0}
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
	db   *bolt.DB
	path string
}

// NewRootStore creates a new bbolt root store
func NewRootStore(config RootStoreConfig) (*RootStore, error) {
	db, err := bolt.Open(config.Path, 0666, nil)

	if err != nil {
		return nil, fmt.Errorf("could not open bbolt store at %s: %s", config.Path, err)
	}

	if err := db.Update(func(txn *bolt.Tx) error {
		_, err := txn.CreateBucketIfNotExists(storesBucket)

		return err
	}); err != nil {
		db.Close()

		return nil, fmt.Errorf("could not ensure stores bucket exists: %s", err)
	}

	return &RootStore{
		db:   db,
		path: config.Path,
	}, nil
}

func (rootStore *RootStore) storesBucket(txn *bolt.Tx) (*bolt.Bucket, error) {
	stores := txn.Bucket(storesBucket)

	if stores == nil {
		return nil, fmt.Errorf("unexpected error: could not retrieve stores bucket")
	}

	return stores, nil
}

// Delete implements RootStore.Delete
func (rootStore *RootStore) Delete() error {
	if err := rootStore.Close(); err != nil {
		return fmt.Errorf("could not close root store: %s", err)
	}

	if err := os.Remove(rootStore.path); err != nil {
		return fmt.Errorf("could not remove directory: %s", err)
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
	stores, err := store.rootStore.storesBucket(txn)

	if err != nil {
		return nil, err
	}

	storeBucket := stores.Bucket(store.name)

	if storeBucket == nil {
		return nil, kv.ErrNoSuchStore
	}

	return storeBucket, nil
}

func (store *Store) partitionsBucket(txn *bolt.Tx) (*bolt.Bucket, error) {
	storeBucket, err := store.bucket(txn)

	if err != nil {
		return nil, err
	}

	partBucket := storeBucket.Bucket(partitionsBucket)

	if partBucket == nil {
		return nil, fmt.Errorf("unexpected error: could not retrieve partitions bucket inside store bucket %s", store.name)
	}

	return partBucket, nil
}

// Name implements Store.Name
func (store *Store) Name() []byte {
	return store.name
}

// Create implements Store.Create
func (store *Store) Create() error {
	err := store.rootStore.db.Update(func(txn *bolt.Tx) error {
		if len(store.name) == 0 {
			return fmt.Errorf("store name cannot be nil or empty")
		}

		stores, err := store.rootStore.storesBucket(txn)

		if err != nil {
			return err
		}

		if bucket, err := stores.CreateBucketIfNotExists(store.name); err != nil {
			return fmt.Errorf("could not create bucket %s: %s", store.name, err)
		} else if _, err := bucket.CreateBucketIfNotExists(partitionsBucket); err != nil {
			return fmt.Errorf("could not create kv bucket inside store bucket %s: %s", store.name, err)
		}

		return nil
	})

	if err == bolt.ErrDatabaseNotOpen {
		return kv.ErrClosed
	}

	return err
}

// Delete implements Store.Delete
func (store *Store) Delete() error {
	err := store.rootStore.db.Update(func(txn *bolt.Tx) error {
		if len(store.name) == 0 {
			return fmt.Errorf("store name cannot be nil or empty")
		}

		stores, err := store.rootStore.storesBucket(txn)

		if err != nil {
			return err
		}

		if err := stores.DeleteBucket(store.name); err != nil {
			if err == bolt.ErrBucketNotFound {
				return nil
			}

			return fmt.Errorf("could not delete bucket %s: %s", store.name, err)
		}

		return nil
	})

	if err == bolt.ErrDatabaseNotOpen {
		return kv.ErrClosed
	}

	return err
}

// Partitions implements Store.Partitions
func (store *Store) Partitions(names keys.Range, limit int) ([][]byte, error) {
	partitions := [][]byte{}

	err := store.rootStore.db.View(func(txn *bolt.Tx) error {
		if len(store.name) == 0 {
			return fmt.Errorf("store name cannot be nil or empty")
		}

		partBucket, err := store.partitionsBucket(txn)

		if err != nil {
			return err
		}

		cursor := partBucket.Cursor()

		var nextPartition []byte

		if names.Min == nil {
			nextPartition, _ = cursor.First()
		} else {
			nextPartition, _ = cursor.Seek(names.Min)
		}

		for nextPartition != nil && (limit < 0 || len(partitions) < limit) && (names.Max == nil || bytes.Compare(nextPartition, names.Max) < 0) {
			partitions = append(partitions, nextPartition)
			nextPartition, _ = cursor.Next()
		}

		return nil
	})

	if err == bolt.ErrDatabaseNotOpen {
		return nil, kv.ErrClosed
	} else if err != nil {
		return nil, err
	}

	return partitions, nil
}

// Partition implements Store.Partition
func (store *Store) Partition(name []byte) kv.Partition {
	partition := &Partition{
		store: store,
		name:  name,
	}

	// Use generic snapshotter
	partition.Snapshotter.Begin = partition.begin
	partition.Snapshotter.Purge = partition.purge

	return partition
}

var _ kv.Partition = (*Partition)(nil)

// Partition implements kv.Partition on top of a bbolt store
// bucket
type Partition struct {
	kv.Snapshotter
	store *Store
	name  []byte
}

func (partition *Partition) bucket(txn *bolt.Tx) (*bolt.Bucket, error) {
	partBucket, err := partition.store.partitionsBucket(txn)

	if err != nil {
		return nil, err
	}

	myBucket := partBucket.Bucket(partition.name)

	if myBucket == nil {
		return nil, kv.ErrNoSuchPartition
	}

	return myBucket, nil
}

func (partition *Partition) purge(transaction kv.Transaction) error {
	txn, ok := transaction.(*Transaction)

	if !ok {
		return fmt.Errorf("transaction must be an instance of bbolt.Transaction")
	}

	partBucket, err := partition.store.partitionsBucket(txn.txn)

	if err != nil {
		return err
	}

	if err := partBucket.DeleteBucket(partition.name); err != nil && err != bolt.ErrBucketNotFound {
		return fmt.Errorf("could not delete partition bucket %s: %s", partition.name, err)
	}

	bucket, _, err := partition.ensureBuckets(txn.txn)

	if err != nil {
		return fmt.Errorf("could not create partition bucket %s: %s", partition.name, err)
	}

	txn.bucket = bucket

	return nil
}

func (partition *Partition) begin(writable bool, ensurePartitionExists bool) (kv.Transaction, error) {
	txn, err := partition.store.rootStore.db.Begin(writable)

	if err == bolt.ErrDatabaseNotOpen {
		return nil, kv.ErrClosed
	} else if err != nil {
		return nil, fmt.Errorf("could not begin bolt transaction: %s", err)
	}

	if len(partition.store.name) == 0 || len(partition.name) == 0 {
		txn.Rollback()

		return nil, fmt.Errorf("neither store name nor partition name can be nil or empty")
	}

	partBucket, err := partition.store.partitionsBucket(txn)

	if err != nil {
		txn.Rollback()

		return nil, err
	}

	partitionBucket := partBucket.Bucket(partition.name)

	if partitionBucket == nil {
		if ensurePartitionExists {
			partitionBucket, _, err = partition.ensureBuckets(txn)

			if err != nil {
				txn.Rollback()

				return nil, fmt.Errorf("could not create partition %s inside store %s: %s", partition.name, partition.store.name, err)
			}
		} else {
			txn.Rollback()

			return nil, kv.ErrNoSuchPartition
		}
	}

	return &Transaction{txn: txn, bucket: partitionBucket, iterators: make(map[*Iterator]bool)}, nil
}

func (partition *Partition) ensureBuckets(txn *bolt.Tx) (*bolt.Bucket, bool, error) {
	partBucket, err := partition.store.partitionsBucket(txn)

	if err != nil {
		return nil, false, err
	}

	exists := false
	myBucket, err := partBucket.CreateBucket(partition.name)

	if err == bolt.ErrBucketExists {
		exists = true
		myBucket = partBucket.Bucket(partition.name)
	} else if err != nil {
		return nil, false, fmt.Errorf("could not create partition %s inside store %s: %s", partition.name, partition.store.name, err)
	}

	_, err = myBucket.CreateBucketIfNotExists(metaBucket)

	if err != nil {
		return nil, false, fmt.Errorf("could not create metadata bucket for partition %s: %s", partition.name, err)
	}

	_, err = myBucket.CreateBucketIfNotExists(kvBucket)

	if err != nil {
		return nil, false, fmt.Errorf("could not create kv bucket for partition %s: %s", partition.name, err)
	}

	return myBucket, exists, nil
}

// Name implements Partition.Name
func (partition *Partition) Name() []byte {
	return partition.name
}

// Create implements Partition.Create
func (partition *Partition) Create(metadata []byte) error {
	err := partition.store.rootStore.db.Update(func(txn *bolt.Tx) error {
		if len(partition.store.name) == 0 || len(partition.name) == 0 {
			return fmt.Errorf("neither store name nor partition name can be nil or empty")
		}

		partitionBucket, exists, err := partition.ensureBuckets(txn)

		if err != nil {
			return err
		}

		if !exists {
			// This bucket was newly created. Set metadata since this
			// is the first time
			transaction := &Transaction{txn: txn, bucket: partitionBucket, iterators: make(map[*Iterator]bool)}

			if err := transaction.SetMetadata(metadata); err != nil {
				return fmt.Errorf("could not set metadata: %s", err)
			}
		}

		return nil
	})

	if err == bolt.ErrDatabaseNotOpen {
		return kv.ErrClosed
	}

	return err
}

// Delete implements Partition.Delete
func (partition *Partition) Delete() error {
	err := partition.store.rootStore.db.Update(func(txn *bolt.Tx) error {
		if len(partition.store.name) == 0 || len(partition.name) == 0 {
			return fmt.Errorf("neither store name nor partition name can be nil or empty")
		}

		partBucket, err := partition.store.partitionsBucket(txn)

		if err != nil {
			return err
		}

		partitionBucket := partBucket.Bucket(partition.name)

		if partitionBucket == nil {
			return nil
		}

		if err := partBucket.DeleteBucket(partition.name); err != nil {
			return fmt.Errorf("could not delete partition bucket %s: %s", partition.name, err)
		}

		return nil
	})

	if err == bolt.ErrDatabaseNotOpen {
		return kv.ErrClosed
	}

	return err
}

// Begin implements Partition.Begin
func (partition *Partition) Begin(writable bool) (kv.Transaction, error) {
	txn, err := partition.store.rootStore.db.Begin(writable)

	if err == bolt.ErrDatabaseNotOpen {
		return nil, kv.ErrClosed
	} else if err != nil {
		return nil, fmt.Errorf("could not begin bolt transaction: %s", err)
	}

	if len(partition.store.name) == 0 || len(partition.name) == 0 {
		txn.Rollback()

		return nil, fmt.Errorf("neither store name nor partition name can be nil or empty")
	}

	partitionBucket, err := partition.bucket(txn)

	if err != nil {
		txn.Rollback()

		return nil, err
	}

	return &Transaction{txn: txn, bucket: partitionBucket, iterators: make(map[*Iterator]bool)}, nil
}

var _ kv.Transaction = (*Transaction)(nil)

// Transaction implements kv.Transaction on top of
// a bbolt transaction.
type Transaction struct {
	txn       *bolt.Tx
	bucket    *bolt.Bucket
	kv        *bolt.Bucket
	meta      *bolt.Bucket
	iterators map[*Iterator]bool
}

func (transaction *Transaction) kvBucket() (*bolt.Bucket, error) {
	if transaction.kv == nil {
		transaction.kv = transaction.bucket.Bucket(kvBucket)

		if transaction.kv == nil {
			return nil, fmt.Errorf("unexpected error: could not retrieve kv bucket")
		}
	}

	return transaction.kv, nil
}

func (transaction *Transaction) metaBucket() (*bolt.Bucket, error) {
	if transaction.meta == nil {
		transaction.meta = transaction.bucket.Bucket(metaBucket)

		if transaction.meta == nil {
			return nil, fmt.Errorf("unexpected error: could not retrieve meta bucket")
		}
	}

	return transaction.meta, nil
}

func (transaction *Transaction) notifyIterators() {
	for iter := range transaction.iterators {
		iter.notify()
	}
}

// Put implements Transaction.Put
func (transaction *Transaction) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("neither key nor value can be empty or nil")
	}

	if value == nil {
		return fmt.Errorf("value cannot be nil")
	}

	bucket, err := transaction.kvBucket()

	if err != nil {
		return err
	}

	transaction.notifyIterators()

	return bucket.Put(key, value)
}

// Get implements Transaction.Get
func (transaction *Transaction) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty or nil")
	}

	bucket, err := transaction.kvBucket()

	if err != nil {
		return nil, err
	}

	return bucket.Get(key), nil
}

// Delete implements Transaction.Delete
func (transaction *Transaction) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty or nil")
	}

	bucket, err := transaction.kvBucket()

	if err != nil {
		return err
	}

	transaction.notifyIterators()

	return bucket.Delete(key)
}

// Metadata implements Transaction.Metadata
func (transaction *Transaction) Metadata() ([]byte, error) {
	bucket, err := transaction.metaBucket()

	if err != nil {
		return nil, err
	}

	return bucket.Get(metaKey), nil
}

// SetMetadata implements Transaction.SetMetadata
func (transaction *Transaction) SetMetadata(metadata []byte) error {
	bucket, err := transaction.metaBucket()

	if err != nil {
		return err
	}

	transaction.notifyIterators()

	return bucket.Put(metaKey, metadata)
}

// Keys implements Transaction.Keys
func (transaction *Transaction) Keys(keys keys.Range, order kv.SortOrder) (kv.Iterator, error) {
	bucket, err := transaction.kvBucket()

	if err != nil {
		return nil, err
	}

	iter := &Iterator{}
	cursor := bucket.Cursor()
	advance := func() ([]byte, []byte) {
		if order == kv.SortOrderDesc {
			return cursor.Prev()
		}

		return cursor.Next()
	}
	seek := func() ([]byte, []byte) {
		k, v := cursor.Seek(iter.key)

		if order == kv.SortOrderDesc {
			return cursor.Prev()
		} else if bytes.Compare(k, iter.key) == 0 {
			return cursor.Next()
		}

		return k, v
	}
	end := func(key []byte) bool {
		if order == kv.SortOrderDesc {
			return keys.Min != nil && bytes.Compare(key, keys.Min) < 0
		}

		return keys.Max != nil && bytes.Compare(key, keys.Max) >= 0
	}

	iter.next = func() ([]byte, []byte) {
		iter.next = func() ([]byte, []byte) {
			var k []byte
			var v []byte

			if iter.seek {
				k, v = seek()
			} else {
				k, v = advance()
			}

			if k == nil || end(k) {
				delete(transaction.iterators, iter)
				return nil, nil
			}

			return k, v
		}

		var k []byte
		var v []byte

		if order == kv.SortOrderDesc {
			if keys.Max == nil {
				k, v = cursor.Last()
			} else {
				cursor.Seek(keys.Max)
				k, v = cursor.Prev()
			}
		} else {
			if keys.Min == nil {
				k, v = cursor.First()
			} else {
				k, v = cursor.Seek(keys.Min)
			}
		}

		if k == nil || end(k) {
			delete(transaction.iterators, iter)
			k, v = nil, nil
		}

		return k, v
	}

	transaction.iterators[iter] = true

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
	seek  bool
	done  bool
}

func (iterator *Iterator) notify() {
	iterator.seek = true
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
