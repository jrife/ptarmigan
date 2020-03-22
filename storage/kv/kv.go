package kv

import (
	"io"
)

type Store interface {
	Close() error
	Delete() error
	SubStore
}

type SubStore interface {
	Begin(writable bool) (Transaction, error)
	Snapshot() (io.ReadCloser, error)
	ApplySnapshot(io.Reader) error
	Namespace(bucket []byte) SubStore
}

type Transaction interface {
	Root() Bucket
	Namespace(bucket []byte) Transaction
	OnCommit(func())
	Commit() error
	Rollback() error
	Size() int64
}

type Bucket interface {
	Bucket(name []byte) Bucket
	CreateBucket(name []byte) (Bucket, error)
	CreateBucketIfNotExists(name []byte) (Bucket, error)
	Cursor() Cursor
	DeleteBucket(name []byte) error
	ForEachBucket(fn func(name []byte, bucket Bucket) error) error
	ForEach(fn func(key []byte, value []byte) error) error
	Delete(key []byte) error
	Get(key []byte) []byte
	NextSequence() (uint64, error)
	Put(key []byte, value []byte) error
	Purge() error
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
