package kv

// import (
// 	"context"
// 	"fmt"
// 	"io"

// 	"github.com/jrife/ptarmigan/storage/kv/kvpb"
// 	"github.com/jrife/ptarmigan/utils/lvstream"
// )

// type Snapshotter struct {
// 	Transaction Transaction
// }

// func (snapshotter *Snapshotter) Snapshot() (io.ReadCloser, error) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	kvPairs, errors := readKVPairs(ctx, snapshotter.Transaction)

// 	lvStreamEncoder := lvstream.NewLVStreamEncoder(func() ([]byte, error) {
// 		select {
// 		case kvPair, ok := <-kvPairs:
// 			if !ok {
// 				return nil, io.EOF
// 			}

// 			marshaled, err := kvPair.Marshal()

// 			if err != nil {
// 				return nil, fmt.Errorf("Could not marshal kv pair: %s", err.Error())
// 			}

// 			return marshaled, nil
// 		case err, ok := <-errors:
// 			if !ok {
// 				return nil, io.EOF
// 			}

// 			return nil, err
// 		}
// 	}, func() {
// 		cancel()
// 		for _ = range kvPairs {
// 		}
// 		snapshotter.Transaction.Rollback()
// 	})

// 	return lvStreamEncoder, nil
// }

// func (snapshotter *Snapshotter) ApplySnapshot(snapshot io.Reader) error {
// 	defer snapshotter.Transaction.Rollback()

// 	kvPairs := make(chan kvpb.KVPair)
// 	ctx, cancel := context.WithCancel(context.Background())

// 	errors := writeKVPairs(ctx, snapshotter.Transaction, kvPairs)

// 	lvStreamDecoder := lvstream.NewLVStreamDecoder(func(value []byte) error {
// 		var kvPair kvpb.KVPair

// 		if err := kvPair.Unmarshal(value); err != nil {
// 			return fmt.Errorf("Could not unmarshal kv pair: %s", err.Error())
// 		}

// 		select {
// 		case kvPairs <- kvPair:
// 			return nil
// 		case err := <-errors:
// 			return err
// 		}
// 	})

// 	_, err := io.Copy(lvStreamDecoder, snapshot)
// 	cancel()

// 	for _ = range errors {
// 	}

// 	if err == nil {
// 		err = snapshotter.Transaction.Commit()

// 		if err != nil {
// 			err = fmt.Errorf("Could not commit transaction: %s", err.Error())
// 		}
// 	} else {
// 		err = fmt.Errorf("Copy failed: %s", err.Error())
// 	}

// 	return err
// }

// func writeKVPairs(ctx context.Context, transaction Transaction, kvPairs <-chan kvpb.KVPair) <-chan error {
// 	errors := make(chan error)

// 	go func() {
// 		defer close(errors)

// 		// Purge the store. Make sure we're starting with a clean slate
// 		err := transaction.Root().Empty()

// 		if err != nil {
// 			errors <- fmt.Errorf("Could not purge store: %s", err.Error())

// 			return
// 		}

// 		// Populate the store with the snapshot
// 		err = writeBucket(ctx, transaction.Root(), kvPairs)

// 		if err != nil {
// 			errors <- err

// 			return
// 		}
// 	}()

// 	return errors
// }

// func writeBucket(ctx context.Context, bucket Bucket, kvPairs <-chan kvpb.KVPair) error {
// 	for {
// 		select {
// 		case kvPair := <-kvPairs:
// 			if kvPair.Key != nil && kvPair.Value != nil {
// 				// This is a regular key-value pair
// 				if err := bucket.Put(kvPair.Key, kvPair.Value); err != nil {
// 					return fmt.Errorf("Could not write key %v: %s", kvPair.Key, err.Error())
// 				}
// 			} else if kvPair.Key != nil && kvPair.Value == nil {
// 				innerBucket := bucket.Bucket(kvPair.Key)
// 				// Bucket start for inner bucket
// 				err := innerBucket.Create()

// 				if err != nil {
// 					return fmt.Errorf("Could not create inner bucket %v: %s", kvPair.Key, err.Error())
// 				}

// 				err = writeBucket(ctx, innerBucket, kvPairs)

// 				if err != nil {
// 					return fmt.Errorf("Could not write inner bucket %v: %s", kvPair.Key, err.Error())
// 				}
// 			} else if kvPair.Key == nil && kvPair.Value == nil {
// 				return nil
// 			} else {
// 				return fmt.Errorf("Expected bucket start or key value pair")
// 			}
// 		case <-ctx.Done():
// 			return context.Canceled
// 		}
// 	}
// }

// func readKVPairs(ctx context.Context, transaction Transaction) (<-chan kvpb.KVPair, <-chan error) {
// 	kvPairs := make(chan kvpb.KVPair)
// 	errors := make(chan error)

// 	go func() {
// 		if err := traverseBucket(ctx, transaction.Root(), kvPairs); err != nil {
// 			errors <- err
// 		}

// 		close(kvPairs)
// 		close(errors)
// 	}()

// 	return kvPairs, errors
// }

// func traverseBucket(ctx context.Context, bucket Bucket, kvPairs chan kvpb.KVPair) error {
// 	cursor := bucket.Cursor()

// 	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
// 		if v == nil {
// 			// This is a bucket.
// 			select {
// 			case kvPairs <- kvpb.KVPair{Key: k}:
// 			case <-ctx.Done():
// 				return context.Canceled
// 			}

// 			if err := traverseBucket(ctx, bucket.Bucket(k), kvPairs); err != nil {
// 				return err
// 			}

// 			select {
// 			case kvPairs <- kvpb.KVPair{}:
// 			case <-ctx.Done():
// 				return context.Canceled
// 			}
// 			continue
// 		}

// 		select {
// 		case kvPairs <- kvpb.KVPair{Key: k, Value: v}:
// 		case <-ctx.Done():
// 			return context.Canceled
// 		}
// 	}

// 	return nil
// }
