package kv

import (
	"context"
	"fmt"
	"io"

	"github.com/jrife/ptarmigan/storage/kv/keys"
	kvpb "github.com/jrife/ptarmigan/storage/kv/pb"
	"github.com/jrife/ptarmigan/utils/lvstream"
)

// Snapshotter contains a generic implementation
// for Snapshot() and ApplySnapshot() that should
// be suitable for most kv plugins. Some plugins
// may offer more efficient ways to do this and may
// choose not to use this implementation.
type Snapshotter struct {
	// Partition should be the partition that is
	// to be snapshotted or to which a snapshot is
	// to be applied
	Begin func(writable bool, ensurePartitionExists bool) (Transaction, error)
	// Purge must be configured to a function which
	// completely erases all data in this partition using
	// the given transaction. Purge is used in the first
	// phase in applying a snapshot where the partition
	// is reverted to a blank slate on top of which the
	// snapshot is written.
	Purge func(transaction Transaction) error
}

// Snapshot implements Partition.Snapshot
func (snapshotter *Snapshotter) Snapshot() (io.ReadCloser, error) {
	transaction, err := snapshotter.Begin(false, false)

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	kvPairs, errors := readKVPairs(ctx, transaction)

	lvStreamEncoder := lvstream.NewLVStreamEncoder(func() ([]byte, error) {
		select {
		case kvPair, ok := <-kvPairs:
			if !ok {
				return nil, io.EOF
			}

			marshaled, err := kvPair.Marshal()

			if err != nil {
				return nil, fmt.Errorf("could not marshal kv pair: %s", err)
			}

			return marshaled, nil
		case err, ok := <-errors:
			if !ok {
				return nil, io.EOF
			}

			return nil, err
		}
	}, func() {
		cancel()
		for range errors {
		}
		for range kvPairs {
		}

		transaction.Rollback()
	})

	return lvStreamEncoder, nil
}

// ApplySnapshot implements Partition.ApplySnapshot
func (snapshotter *Snapshotter) ApplySnapshot(snapshot io.Reader) error {
	transaction, err := snapshotter.Begin(true, true)

	if err != nil {
		return err
	}

	defer transaction.Rollback()

	if err := snapshotter.Purge(transaction); err != nil {
		return fmt.Errorf("could not purge partition: %s", err)
	}

	kvPairs := make(chan kvpb.KVPair)
	ctx, cancel := context.WithCancel(context.Background())

	errors := writeKVPairs(ctx, transaction, kvPairs)

	lvStreamDecoder := lvstream.NewLVStreamDecoder(func(value []byte) error {
		var kvPair kvpb.KVPair

		if err := kvPair.Unmarshal(value); err != nil {
			return fmt.Errorf("could not unmarshal kv pair: %s", err)
		}

		select {
		case kvPairs <- kvPair:
			return nil
		case err := <-errors:
			return err
		}
	})

	_, err = io.Copy(lvStreamDecoder, snapshot)
	cancel()

	for range errors {
	}

	if err == nil {
		err = transaction.Commit()

		if err != nil {
			err = fmt.Errorf("could not commit transaction: %s", err)
		}
	} else {
		err = fmt.Errorf("copy failed: %s", err)
	}

	return err
}

func writeKVPairs(ctx context.Context, transaction Transaction, kvPairs <-chan kvpb.KVPair) <-chan error {
	errors := make(chan error)

	go func() {
		defer close(errors)

		select {
		case kvPair := <-kvPairs:
			if err := transaction.SetMetadata(kvPair.Value); err != nil {
				errors <- fmt.Errorf("could not write metadata %v: %s", kvPair.Value, err)

				return
			}
		case <-ctx.Done():
			errors <- context.Canceled

			return
		}

		for {
			select {
			case kvPair := <-kvPairs:
				if err := transaction.Put(kvPair.Key, kvPair.Value); err != nil {
					errors <- fmt.Errorf("could not write key %v: %s", kvPair.Key, err)

					return
				}
			case <-ctx.Done():
				errors <- context.Canceled

				return
			}
		}
	}()

	return errors
}

func readKVPairs(ctx context.Context, transaction Transaction) (<-chan kvpb.KVPair, <-chan error) {
	kvPairs := make(chan kvpb.KVPair)
	errors := make(chan error)

	go func() {
		defer close(kvPairs)
		defer close(errors)

		metadata, err := transaction.Metadata()

		if err != nil {
			errors <- fmt.Errorf("could not create iterator: %s", err)

			return
		}

		select {
		case kvPairs <- kvpb.KVPair{Key: []byte{}, Value: metadata}:
		case <-ctx.Done():
			errors <- context.Canceled

			return
		}

		iter, err := transaction.Keys(keys.All(), SortOrderAsc)

		if err != nil {
			errors <- fmt.Errorf("could not create iterator: %s", err)

			return
		}

		for iter.Next() {
			select {
			case kvPairs <- kvpb.KVPair{Key: iter.Key(), Value: iter.Value()}:
			case <-ctx.Done():
				errors <- context.Canceled

				return
			}
		}

		if iter.Error() != nil {
			errors <- fmt.Errorf("iteration error: %s", err)

			return
		}
	}()

	return kvPairs, errors
}
