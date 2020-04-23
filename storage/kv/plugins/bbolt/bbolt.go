package bbolt

import (
	"fmt"
	"io"
	"os"

	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/utils/uuid"
	bolt "go.etcd.io/bbolt"
)

const (
	DriverName = "bbolt"
)

func Plugins() []kv.Plugin {
	return []kv.Plugin{
		&BBoltPlugin{},
	}
}

type BBoltPlugin struct {
}

func (plugin *BBoltPlugin) Init(options kv.PluginOptions) error {
	return nil
}

func (plugin *BBoltPlugin) Name() string {
	return DriverName
}

func (plugin *BBoltPlugin) NewStore(options kv.PluginOptions) (kv.Store, error) {
	var config BBoltStoreConfig

	if path, ok := options["path"]; !ok {
		return nil, fmt.Errorf("\"path\" is required")
	} else if pathString, ok := path.(string); !ok {
		return nil, fmt.Errorf("\"path\" must be a string")
	} else {
		config.Path = pathString
	}

	store, err := New(config)

	if err != nil {
		return nil, err
	}

	return store, nil
}

func (plugin *BBoltPlugin) NewTempStore() (kv.Store, error) {
	return plugin.NewStore(kv.PluginOptions{
		"path": fmt.Sprintf("/tmp/bbolt-%s", uuid.MustUUID()),
	})
}

type BBoltStoreConfig struct {
	Path string
}

var _ kv.Store = (*BBoltStore)(nil)

func New(config BBoltStoreConfig) (*BBoltStore, error) {
	db, err := bolt.Open(config.Path, 0666, nil)

	if err != nil {
		return nil, fmt.Errorf("Could not open bbolt store at %s: %s", config.Path, err.Error())
	}

	if err := db.Update(func(txn *bolt.Tx) error {
		_, err := txn.CreateBucketIfNotExists([]byte{0})

		return err
	}); err != nil {
		db.Close()

		return nil, fmt.Errorf("Could not ensure root bucket exists: %s", err.Error())
	}

	return &BBoltStore{
		BBoltSubStore: BBoltSubStore{
			db:        db,
			namespace: [][]byte{[]byte{0}},
		},
	}, nil
}

type BBoltStore struct {
	BBoltSubStore
}

func (store *BBoltStore) Close() error {
	return store.db.Close()
}

func (store *BBoltStore) Delete() error {
	path := store.db.Path()

	if err := store.Close(); err != nil {
		return fmt.Errorf("Could not close store: %s", err.Error())
	}

	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("Could not remove path %s: %s", path, err.Error())
	}

	return nil
}

var _ kv.SubStore = (*BBoltSubStore)(nil)

type BBoltSubStore struct {
	db        *bolt.DB
	namespace [][]byte
}

func (store *BBoltSubStore) Begin(writable bool) (kv.Transaction, error) {
	transaction, err := store.db.Begin(writable)

	if err != nil {
		return nil, fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	txn := &BBoltTransaction{transaction: transaction, namespace: store.namespace}

	return txn, nil
}

func (store *BBoltSubStore) Snapshot() (io.ReadCloser, error) {
	transaction, err := store.Begin(false)

	if err != nil {
		return nil, fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	snapshotter := kv.Snapshotter{Transaction: transaction}

	return snapshotter.Snapshot()
}

func (store *BBoltSubStore) ApplySnapshot(snapshot io.Reader) error {
	transaction, err := store.Begin(true)

	if err != nil {
		return fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	snapshotter := kv.Snapshotter{Transaction: transaction}

	err = snapshotter.ApplySnapshot(snapshot)

	if err != nil {
		return fmt.Errorf("Could not apply snapshot: %s", err.Error())
	}

	return nil
}

func (store *BBoltSubStore) Namespace(bucket []byte) kv.SubStore {
	namespace := make([][]byte, len(store.namespace)+1)

	copy(namespace, store.namespace)
	namespace[len(namespace)-1] = bucket

	return &BBoltSubStore{db: store.db, namespace: namespace}
}

var _ kv.Transaction = (*BBoltTransaction)(nil)

type BBoltTransaction struct {
	transaction *bolt.Tx
	namespace   [][]byte
}

func (transaction *BBoltTransaction) bucket() *bolt.Bucket {
	var bucket *bolt.Bucket = transaction.transaction.Bucket(transaction.namespace[0])

	for i := 1; i < len(transaction.namespace); i++ {
		if bucket == nil {
			return nil
		}

		bucket = bucket.Bucket(transaction.namespace[i])
	}

	return bucket
}

func (transaction *BBoltTransaction) Root() kv.Bucket {
	if transaction.bucket() == nil {
		return nil
	}

	return &BBoltBucket{bucket: transaction.bucket()}
}

func (transaction *BBoltTransaction) Namespace(bucket []byte) kv.Transaction {
	namespace := make([][]byte, len(transaction.namespace)+1)

	copy(namespace, transaction.namespace)
	namespace[len(namespace)-1] = bucket

	txn := &BBoltTransaction{
		transaction: transaction.transaction,
		namespace:   namespace,
	}

	return txn
}

func (transaction *BBoltTransaction) Commit() error {
	return transaction.transaction.Commit()
}

func (transaction *BBoltTransaction) OnCommit(cb func()) {
	transaction.transaction.OnCommit(cb)
}

func (transaction *BBoltTransaction) Rollback() error {
	return transaction.transaction.Rollback()
}

var _ kv.Bucket = (*BBoltBucket)(nil)

type BBoltBucket struct {
	parent *bolt.Bucket
	bucket *bolt.Bucket
	name   []byte
}

func (bucket *BBoltBucket) Bucket(name []byte) kv.Bucket {
	if bucket.bucket.Bucket(name) == nil {
		return nil
	}

	return &BBoltBucket{parent: bucket.bucket, bucket: bucket.bucket.Bucket(name)}
}

func (bucket *BBoltBucket) Create() error {
	if bucket.parent == nil {
		// This is the root bucket.
		return nil
	}

	b, err := bucket.parent.CreateBucket(bucket.name)

	if err != nil {
		return err
	}

	bucket.bucket = b

	return nil
}

func (bucket *BBoltBucket) Purge() error {
	if bucket.parent == nil {
		// This is the root bucket.
		return nil
	}

	return bucket.parent.DeleteBucket(bucket.name)
}

func (bucket *BBoltBucket) Cursor() kv.Cursor {
	return &BBoltCursor{cursor: bucket.bucket.Cursor()}
}

func (bucket *BBoltBucket) ForEachBucket(fn func(name []byte, bucket kv.Bucket) error) error {
	cursor := bucket.bucket.Cursor()

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if v == nil {
			if err := fn(k, &BBoltBucket{parent: bucket.bucket, bucket: bucket.bucket.Bucket(k)}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bucket *BBoltBucket) ForEach(fn func(key []byte, value []byte) error) error {
	return bucket.bucket.ForEach(fn)
}

func (bucket *BBoltBucket) Delete(key []byte) error {
	return bucket.bucket.Delete(key)
}

func (bucket *BBoltBucket) Get(key []byte) ([]byte, error) {
	return bucket.bucket.Get(key), nil
}

func (bucket *BBoltBucket) NextSequence() (uint64, error) {
	return bucket.bucket.NextSequence()
}

func (bucket *BBoltBucket) Put(key []byte, value []byte) error {
	return bucket.bucket.Put(key, value)
}

func (bucket *BBoltBucket) Empty() error {
	cursor := bucket.Cursor()

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if v == nil {
			if err := bucket.Bucket(k).Purge(); err != nil {
				return fmt.Errorf("Could not delete bucket %v: %s", k, err.Error())
			}
		} else {
			if err := bucket.Delete(k); err != nil {
				return fmt.Errorf("Could not delete key %v: %s", k, err.Error())
			}
		}
	}

	return nil
}

var _ kv.Cursor = (*BBoltCursor)(nil)

type BBoltCursor struct {
	cursor *bolt.Cursor
}

func (cursor *BBoltCursor) Error() error {
	return nil
}

func (cursor *BBoltCursor) Delete() error {
	return cursor.cursor.Delete()
}

func (cursor *BBoltCursor) First() (key []byte, value []byte) {
	return cursor.cursor.First()
}

func (cursor *BBoltCursor) Last() (key []byte, value []byte) {
	return cursor.cursor.Last()
}

func (cursor *BBoltCursor) Next() (key []byte, value []byte) {
	return cursor.cursor.Next()
}

func (cursor *BBoltCursor) Prev() (key []byte, value []byte) {
	return cursor.cursor.Prev()
}

func (cursor *BBoltCursor) Seek(seek []byte) (key []byte, value []byte) {
	return cursor.cursor.Seek(seek)
}
