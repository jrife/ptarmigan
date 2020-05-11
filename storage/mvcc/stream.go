package mvcc

import (
	"github.com/jrife/ptarmigan/utils/stream"
)

// Stream wraps the iterator in a stream
// whose values are UnmarshaledKV instances
func Stream(iter UnmarshaledIterator) stream.Stream {
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
