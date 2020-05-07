package mvcc

import (
	"github.com/jrife/ptarmigan/utils/stream"
)

// Stream wraps the iterator in a stream
// whose values are KV instances
func Stream(iter Iterator) stream.Stream {
	return &kvStream{iter}
}

type kvStream struct {
	iter Iterator
}

func (stream *kvStream) Next() bool {
	return stream.iter.Next()
}

func (stream *kvStream) Value() interface{} {
	return KV{stream.iter.Key(), stream.iter.Value()}
}

func (stream *kvStream) Error() error {
	return stream.iter.Error()
}

// StreamMarshaled wraps the iterator in a stream
// whose values are UnmarshaledKV instances
func StreamMarshaled(iter UnmarshaledIterator) stream.Stream {
	return &unmarshaledKVStream{iter}
}

type unmarshaledKVStream struct {
	iter UnmarshaledIterator
}

func (stream *unmarshaledKVStream) Next() bool {
	return stream.iter.Next()
}

func (stream *unmarshaledKVStream) Value() interface{} {
	return UnmarshaledKV{stream.iter.Key(), stream.iter.Value()}
}

func (stream *unmarshaledKVStream) Error() error {
	return stream.iter.Error()
}
