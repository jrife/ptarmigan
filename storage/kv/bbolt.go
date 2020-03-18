package kv

import (
	"fmt"
	"io"

	bolt "go.etcd.io/bbolt"
)

var _ Store = (*BBoltStore)(nil)

type BBoltStore struct {
	db        *bolt.DB
	namespace [][]byte
}

func (store *BBoltStore) Begin(writable bool) (Transaction, error) {
	transaction, err := store.db.Begin(writable)

	if err != nil {
		return nil, fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	return &BBoltTransaction{transaction: transaction, namespace: store.namespace}, nil
}

func (store *BBoltStore) SubStore(bucket []byte) Store {
	namespace := make([][]byte, len(store.namespace)+1)

	copy(namespace, store.namespace)
	namespace[len(namespace)-1] = bucket

	return &BBoltStore{db: store.db, namespace: namespace}
}

var _ Transaction = (*BBoltTransaction)(nil)

type BBoltTransaction struct {
	transaction *bolt.Tx
	namespace   [][]byte
}

func (transaction *BBoltTransaction) bucket() *bolt.Bucket {
	var bucket *bolt.Bucket

	for _, b := range transaction.namespace {
		bucket = transaction.transaction.Bucket(b)
	}

	return bucket
}

func (transaction *BBoltTransaction) Bucket(name []byte) Bucket {
	nsBucket := transaction.bucket()

	if nsBucket == nil {
		return &BBoltBucket{bucket: transaction.transaction.Bucket(name)}
	}

	return &BBoltBucket{bucket: nsBucket.Bucket(name)}
}

func (transaction *BBoltTransaction) Namespace(bucket []byte) Transaction {
	namespace := make([][]byte, len(transaction.namespace)+1)

	copy(namespace, transaction.namespace)
	namespace[len(namespace)-1] = bucket

	return &BBoltTransaction{
		transaction: transaction.transaction,
		namespace:   namespace,
	}
}

func (transaction *BBoltTransaction) CreateBucket(name []byte) (Bucket, error) {
	var bucket *bolt.Bucket
	var err error

	nsBucket := transaction.bucket()

	if nsBucket == nil {
		bucket, err = transaction.transaction.CreateBucket(name)
	} else {
		bucket, err = nsBucket.CreateBucket(name)
	}

	if err != nil {
		return nil, fmt.Errorf("Could not create bucket: %s", err.Error())
	}

	return &BBoltBucket{bucket: bucket}, nil
}

func (transaction *BBoltTransaction) CreateBucketIfNotExists(name []byte) (Bucket, error) {
	var bucket *bolt.Bucket
	var err error

	nsBucket := transaction.bucket()

	if nsBucket == nil {
		bucket, err = transaction.transaction.CreateBucketIfNotExists(name)
	} else {
		bucket, err = nsBucket.CreateBucketIfNotExists(name)
	}

	if err != nil {
		return nil, fmt.Errorf("Could not create bucket: %s", err.Error())
	}

	return &BBoltBucket{bucket: bucket}, nil
}

func (transaction *BBoltTransaction) Cursor() Cursor {
	nsBucket := transaction.bucket()

	if nsBucket == nil {
		return &BBoltCursor{cursor: transaction.transaction.Cursor()}
	}

	return &BBoltCursor{cursor: nsBucket.Cursor()}
}

func (transaction *BBoltTransaction) ForEach(fn func(name []byte, bucket Bucket) error) error {
	nsBucket := transaction.bucket()

	if nsBucket != nil {
		return nil
	}

	return transaction.transaction.ForEach(func(name []byte, b *bolt.Bucket) error {
		return fn(name, &BBoltBucket{bucket: b})
	})
}

func (transaction *BBoltTransaction) DeleteBucket(name []byte) error {
	nsBucket := transaction.bucket()

	if nsBucket == nil {
		return transaction.transaction.DeleteBucket(name)
	}

	return nsBucket.DeleteBucket(name)
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

func (transaction *BBoltTransaction) Size() int64 {
	return transaction.transaction.Size()
}

func (transaction *BBoltTransaction) Snapshot() (io.ReadCloser, error) {
	return nil, nil
}

func (transaction *BBoltTransaction) ApplySnapshot(io.Reader) error {
	return nil
}

var _ Bucket = (*BBoltBucket)(nil)

type BBoltBucket struct {
	bucket *bolt.Bucket
}

func (bucket *BBoltBucket) Bucket(name []byte) Bucket {
	return &BBoltBucket{bucket: bucket.bucket.Bucket(name)}
}

func (bucket *BBoltBucket) CreateBucket(name []byte) (Bucket, error) {
	b, err := bucket.bucket.CreateBucket(name)

	if err != nil {
		return nil, fmt.Errorf("Could not create bucket: %s", err.Error())
	}

	return &BBoltBucket{bucket: b}, nil
}

func (bucket *BBoltBucket) CreateBucketIfNotExists(name []byte) (Bucket, error) {
	b, err := bucket.bucket.CreateBucketIfNotExists(name)

	if err != nil {
		return nil, fmt.Errorf("Could not create bucket: %s", err.Error())
	}

	return &BBoltBucket{bucket: b}, nil
}

func (bucket *BBoltBucket) Cursor() Cursor {
	return &BBoltCursor{cursor: bucket.bucket.Cursor()}
}

func (bucket *BBoltBucket) ForEach(fn func(key []byte, value []byte) error) error {
	return bucket.bucket.ForEach(fn)
}

func (bucket *BBoltBucket) DeleteBucket(name []byte) error {
	return bucket.bucket.Delete(name)
}

func (bucket *BBoltBucket) Delete(key []byte) error {
	return bucket.bucket.Delete(key)
}

func (bucket *BBoltBucket) Get(key []byte) []byte {
	return bucket.bucket.Get(key)
}

func (bucket *BBoltBucket) NextSequence() (uint64, error) {
	return bucket.bucket.NextSequence()
}

func (bucket *BBoltBucket) Put(key []byte, value []byte) error {
	return bucket.bucket.Put(key, value)
}

var _ Cursor = (*BBoltCursor)(nil)

type BBoltCursor struct {
	cursor *bolt.Cursor
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
