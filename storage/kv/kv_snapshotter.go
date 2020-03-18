package kv

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jrife/ptarmigan/storage/kv/kvpb"
	"github.com/jrife/ptarmigan/utils/lvstream"
)

// ApplySnapshot(): io.Reader -> kvPairStream
// Snapshot():                -> kvPairStream -> io.Reader

type kvSnapshotter struct {
	store Store
}

func (snapshotter *kvSnapshotter) Snapshot() (io.Reader, error) {
	return nil, nil
}

func (snapshotter *kvSnapshotter) ApplySnapshot(snapshot io.Reader) error {
	return nil
}

type kvPairStream interface {
	Next() bool
	Error() error
	IsBucketStart() bool
	IsBucketEnd() bool
	Key() []byte
	Value() []byte
	Close() error
}

var _ kvPairStream = (*kvPairEncoder)(nil)
var _ kvPairStream = (*kvPairDecoder)(nil)

// kvPairEncoder encodes a store into
// a series of kv pairs
type kvPairEncoder struct {
	store          Store
	transaction    Transaction
	cursor         Cursor
	lvStreamReader lvstream.LVStreamReader
}

func (encoder *kvPairEncoder) lazyInit() error {
	if encoder.transaction != nil {
		return nil
	}

	transaction, err := encoder.store.Begin(false)

	if err != nil {
		return err
	}

	cursor := transaction.Cursor()
	cursor.First()

	encoder.transaction = transaction
	encoder.cursor = cursor

	return nil
}

func (encoder *kvPairEncoder) Next() bool {
	return false
}

func (encoder *kvPairEncoder) Error() error {
	return nil
}

func (encoder *kvPairEncoder) IsBucketStart() bool {
	return false
}

func (encoder *kvPairEncoder) IsBucketEnd() bool {
	return false
}

func (encoder *kvPairEncoder) Key() []byte {
	return nil
}

func (encoder *kvPairEncoder) Value() []byte {
	return nil
}

func (encoder *kvPairEncoder) Close() error {
	return nil
}

// kvPairDecoder decodes a reader into
// a series of kv pairs
type kvPairDecoder struct {
	reader         io.Reader
	ctx            context.Context
	err            error
	cancel         func()
	values         chan kvpb.KVPair
	current        kvpb.KVPair
	lvStreamWriter *lvstream.LVStreamWriter
}

func (decoder *kvPairDecoder) lazyInit() error {
	if decoder.lvStreamWriter != nil {
		return nil
	}

	decoder.ctx, decoder.cancel = context.WithCancel(context.Background())
	decoder.lvStreamWriter = &lvstream.LVStreamWriter{
		WriteValue: decoder.acceptNewValue,
	}

	go func() {
		if _, err := io.Copy(decoder.lvStreamWriter, decoder.reader); err != nil {
			decoder.err = err
			decoder.cancel()
		}
	}()

	return nil
}

func (decoder *kvPairDecoder) acceptNewValue(value []byte) error {
	var kvPair kvpb.KVPair

	if err := kvPair.Unmarshal(value); err != nil {
		return fmt.Errorf("Could not unmarshal kv pair: %s", err.Error())
	}

	select {
	case decoder.values <- kvPair:
	case <-decoder.ctx.Done():
		return errors.New("Closed")
	}

	return nil
}

func (decoder *kvPairDecoder) Next() bool {
	select {
	case kvPair := <-decoder.values:
		decoder.current = kvPair

		return true
	case <-decoder.ctx.Done():
		return false
	}
}

func (decoder *kvPairDecoder) Error() error {
	return nil
}

func (decoder *kvPairDecoder) IsBucketStart() bool {
	return false
}

func (decoder *kvPairDecoder) IsBucketEnd() bool {
	return false
}

func (decoder *kvPairDecoder) Key() []byte {
	return nil
}

func (decoder *kvPairDecoder) Value() []byte {
	return nil
}

func (decoder *kvPairDecoder) Close() error {
	decoder.cancel()

	return nil
}

func writeKVPairs(store Store, kvPairs <-chan kvpb.KVPair, errors <-chan error) error {
	transaction, err := store.Begin(true)

	if err != nil {
		return fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	defer transaction.Rollback()

	// Purge the store. Make sure we're starting with a clean slate
	err = transaction.ForEach(func(name []byte, bucket Bucket) error {
		return transaction.DeleteBucket(name)
	})

	if err != nil {
		return fmt.Errorf("Could not purge store: %s", err.Error())
	}

	// Populate the store with the snapshot
	err = traverseKVPairsRoot(kvPairs, errors, transaction)

	if err != nil {
		return err
	}

	err = transaction.Commit()

	if err != nil {
		return fmt.Errorf("Could not commit transaction: %s", err.Error())
	}

	return nil
}

func traverseKVPairsRoot(kvPairs <-chan kvpb.KVPair, errors <-chan error, bucket BucketRoot) error {
	for {
		select {
		case kvPair := <-kvPairs:
			if kvPair.Key == nil {
				// End of a bucket
				return nil
			} else if kvPair.Value == nil {
				// Start of a bucket
				innerBucket, err := bucket.CreateBucket(kvPair.Key)

				if err != nil {
					return fmt.Errorf("Could not create bucket %v: %s", kvPair.Key, err.Error())
				}

				if err = traverseKVPairs(kvPairs, errors, innerBucket); err != nil {
					return fmt.Errorf("Error while travering inner bucket %v: %s", kvPair.Key, err.Error())
				}
			} else {
			}
		case err := <-errors:
			return err
		}
	}
}

func traverseKVPairs(kvPairs <-chan kvpb.KVPair, errors <-chan error, bucket Bucket) error {
	for {
		select {
		case kvPair := <-kvPairs:
			if kvPair.Key == nil {
				// End of a bucket
				return nil
			} else if kvPair.Value == nil {
				// Start of a bucket
				innerBucket, err := bucket.CreateBucket(kvPair.Key)

				if err != nil {
					return fmt.Errorf("Could not create bucket %v: %s", kvPair.Key, err.Error())
				}

				if err = traverseKVPairs(kvPairs, errors, innerBucket); err != nil {
					return fmt.Errorf("Error while travering inner bucket %v: %s", kvPair.Key, err.Error())
				}
			} else {
			}
		case err := <-errors:
			return err
		}
	}
}

func readKVPairs(store Store) (<-chan kvpb.KVPair, error) {
	transaction, err := store.Begin(false)

	if err != nil {
		return nil, fmt.Errorf("Could not begin transaction: %s", err.Error())
	}

	kvPairs := make(chan kvpb.KVPair)

	go func() {
		traverseBucket(transaction, kvPairs)
		transaction.Rollback()
		close(kvPairs)
	}()

	return kvPairs, nil
}

func traverseBucket(bucket BucketRoot, kvPairs chan kvpb.KVPair) {
	cursor := bucket.Cursor()

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		// This is a bucket.
		if v == nil {
			kvPairs <- kvpb.KVPair{Key: k}
			traverseBucket(bucket.Bucket(k), kvPairs)
			kvPairs <- kvpb.KVPair{}
			continue
		}

		kvPairs <- kvpb.KVPair{Key: k, Value: v}
	}
}

type KVPairsDecoder struct {
	Reader io.Reader
}

func (decoder *KVPairsDecoder) Decode() (<-chan kvpb.KVPair, <-chan error) {
	return nil, nil
}

var _ io.Reader = (*KVPairsEncoder)(nil)

type KVPairsEncoder struct {
	lvstream.LVStream
	KVPairs <-chan kvpb.KVPair
	Error   <-chan error
}

func (encoder *KVPairsEncoder) Read(p []byte) (n int, err error) {
	if encoder.NextMessage == nil {
		encoder.NextMessage = encoder.nextMessage
	}

	return encoder.LVStream.Read(p)
}

func (encoder *KVPairsEncoder) nextMessage() ([]byte, error) {
	select {
	case kvPair, ok := <-encoder.KVPairs:
		if !ok {
			return nil, io.EOF
		}

		data, err := kvPair.Marshal()

		if err != nil {
			return nil, fmt.Errorf("Could not marshal kv pair: %s", err.Error())
		}

		return data, nil
	case err := <-encoder.Error:
		return nil, err
	}
}
