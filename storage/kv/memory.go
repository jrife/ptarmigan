package kv

import (
	"io"
	"sync"
)

var _ Store = (*MemoryStore)(nil)

type MemoryStore struct {
	mu sync.RWMutex
}

func (store *MemoryStore) Begin(writable bool) (Transaction, error) {
	return &MemoryTransaction{}, nil
}

func (store *MemoryStore) SubStore(bucket []byte) Store {
	return &MemoryStore{}
}

var _ Transaction = (*MemoryTransaction)(nil)

type MemoryTransaction struct {
}

func (transaction *MemoryTransaction) Bucket(name []byte) Bucket {
	return &MemoryBucket{}
}

func (transaction *MemoryTransaction) Namespace(bucket []byte) Transaction {
	return nil
}

func (transaction *MemoryTransaction) CreateBucket(name []byte) (Bucket, error) {
	return &MemoryBucket{}, nil
}

func (transaction *MemoryTransaction) CreateBucketIfNotExists(name []byte) (Bucket, error) {
	return &MemoryBucket{}, nil
}

func (transaction *MemoryTransaction) Cursor() Cursor {
	return &MemoryCursor{}
}

func (transaction *MemoryTransaction) ForEach(fn func(name []byte, bucket Bucket) error) error {
	return nil
}

func (transaction *MemoryTransaction) DeleteBucket(name []byte) error {
	return nil
}

func (transaction *MemoryTransaction) Commit() error {
	return nil
}

func (transaction *MemoryTransaction) OnCommit(cb func()) {
}

func (transaction *MemoryTransaction) Rollback() error {
	return nil
}

func (transaction *MemoryTransaction) Size() int64 {
	return 0
}

func (transaction *MemoryTransaction) Snapshot() (io.ReadCloser, error) {
	return nil, nil
}

func (transaction *MemoryTransaction) ApplySnapshot(io.Reader) error {
	return nil
}

var _ Bucket = (*MemoryBucket)(nil)

type MemoryBucket struct {
}

func (bucket *MemoryBucket) Bucket(name []byte) Bucket {
	return &MemoryBucket{}
}

func (bucket *MemoryBucket) CreateBucket(name []byte) (Bucket, error) {
	return &MemoryBucket{}, nil
}

func (bucket *MemoryBucket) CreateBucketIfNotExists(name []byte) (Bucket, error) {
	return &MemoryBucket{}, nil
}

func (bucket *MemoryBucket) Cursor() Cursor {
	return &MemoryCursor{}
}

func (bucket *MemoryBucket) ForEach(fn func(key []byte, value []byte) error) error {
	return nil
}

func (bucket *MemoryBucket) DeleteBucket(name []byte) error {
	return nil
}

func (bucket *MemoryBucket) Delete(key []byte) error {
	return nil
}

func (bucket *MemoryBucket) Get(key []byte) []byte {
	return nil
}

func (bucket *MemoryBucket) NextSequence() (uint64, error) {
	return 0, nil
}

func (bucket *MemoryBucket) Put(key []byte, value []byte) error {
	return nil
}

var _ Cursor = (*MemoryCursor)(nil)

type MemoryCursor struct {
}

func (cursor *MemoryCursor) Delete() error {
	return nil
}

func (cursor *MemoryCursor) First() (key []byte, value []byte) {
	return nil, nil
}

func (cursor *MemoryCursor) Last() (key []byte, value []byte) {
	return nil, nil
}

func (cursor *MemoryCursor) Next() (key []byte, value []byte) {
	return nil, nil
}

func (cursor *MemoryCursor) Prev() (key []byte, value []byte) {
	return nil, nil
}

func (cursor *MemoryCursor) Seek(seek []byte) (key []byte, value []byte) {
	return nil, nil
}
