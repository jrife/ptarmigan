package kv

import "io"

type Store interface {
	Begin(writable bool) (Transaction, error)
	SubStore(bucket []byte) Store
}

type BucketRoot interface {
	Bucket(name []byte) Bucket
	CreateBucket(name []byte) (Bucket, error)
	CreateBucketIfNotExists(name []byte) (Bucket, error)
	Cursor() Cursor
	DeleteBucket(name []byte) error
}

type Transaction interface {
	BucketRoot
	ForEach(fn func(name []byte, b Bucket) error) error
	Namespace(bucket []byte) Transaction
	OnCommit(func())
	Commit() error
	Rollback() error
	Size() int64
	Snapshot() (io.ReadCloser, error)
	ApplySnapshot(io.Reader) error
}

type Bucket interface {
	BucketRoot
	ForEach(fn func(key []byte, value []byte) error) error
	Delete(key []byte) error
	Get(key []byte) []byte
	NextSequence() (uint64, error)
	Put(key []byte, value []byte) error
}

type BucketTransaction interface {
	Bucket
	Commit() error
	Rollback() error
}

type Cursor interface {
	Delete() error
	First() (key []byte, value []byte)
	Last() (key []byte, value []byte)
	Next() (key []byte, value []byte)
	Prev() (key []byte, value []byte)
	Seek(seek []byte) (key []byte, value []byte)
}
