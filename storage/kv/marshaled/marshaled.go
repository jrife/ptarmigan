package kv

import (
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/utils/stream"
)

// Marshalable describes a type that
// can be marshaled to a series of bytes
type Marshalable interface {
	Marshal() ([]byte, error)
}

// Unmarshaler is a function that unmarshals bytes into some type
type Unmarshaler func(data []byte) (interface{}, error)

// Map combines MapUpdater and MapReader
type Map struct {
	MapUpdater
	MapReader
}

// MapUpdater is like kv.MapUpdater except it marshals the values
type MapUpdater struct {
	kv.MapUpdater
}

// Put is like kv.MapUpdater.Put except it marshals the value
func (m *MapUpdater) Put(key []byte, value Marshalable) error {
	marshaledValue, err := value.Marshal()

	if err != nil {
		return err
	}

	return m.MapUpdater.Put(key, marshaledValue)
}

// MapReader is like kv.MapReader except it marshals the values
type MapReader struct {
	kv.MapReader
	Unmarshal Unmarshaler
}

// Get is like kv.MapReader.Get except it unmarshals the value
func (m *MapReader) Get(key []byte) (interface{}, error) {
	value, err := m.MapReader.Get(key)

	if err != nil {
		return nil, err
	}

	return m.Unmarshal(value)
}

// Keys is like kv.MapReader.Keys except the returned iterator unmarshals values
func (m *MapReader) Keys(keys keys.Range, order kv.SortOrder) (*Iterator, error) {
	iter, err := m.MapReader.Keys(keys, order)

	if err != nil {
		return nil, err
	}

	return &Iterator{
		Iterator:  iter,
		unmarshal: m.Unmarshal,
	}, nil
}

// Iterator is like kv.Iterator except it unmarshals values
type Iterator struct {
	kv.Iterator
	unmarshal Unmarshaler
	value     interface{}
	err       error
}

// Next is like kv.Iterator.Next
func (iterator *Iterator) Next() bool {
	if iterator.err != nil {
		return false
	}

	if !iterator.Iterator.Next() {
		iterator.value = nil
		iterator.err = iterator.Iterator.Error()

		return false
	}

	iterator.value, iterator.err = iterator.unmarshal(iterator.Iterator.Value())

	return iterator.err == nil
}

// Value returns the unmarshaled value at the current iterator position
func (iterator *Iterator) Value() interface{} {
	return iterator.value
}

// KV represents a key-value pair whose
// value has been unmarshaled
type KV struct {
	key   []byte
	value interface{}
}

// Key returns the key
func (kv KV) Key() []byte {
	return kv.key
}

// Value returns the value
func (kv KV) Value() interface{} {
	return kv.value
}

// Stream wraps the iterator in a stream
// whose values are KV instances
func Stream(iter *Iterator) stream.Stream {
	return &kvStream{iter}
}

type kvStream struct {
	iter *Iterator
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
