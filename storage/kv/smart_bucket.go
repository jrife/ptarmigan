package kv

// import (
// 	"errors"
// 	"fmt"
// )

// var ENoSuchKey error = errors.New("No such key")

// type SmartBucketCodec interface {
// 	Marshal(v interface{}) ([]byte, error)
// 	Unmarshal(v []byte) (interface{}, error)
// }

// type SmartBucket struct {
// 	Bucket     Bucket
// 	KeyCodec   SmartBucketCodec
// 	ValueCodec SmartBucketCodec
// }

// func (smartBucket *SmartBucket) ForEach(fn func(key interface{}, value interface{}) error) error {
// 	return smartBucket.Bucket.ForEach(func(k []byte, v []byte) error {
// 		key, err := smartBucket.KeyCodec.Unmarshal(k)

// 		if err != nil {
// 			return fmt.Errorf("Could not unmarshal key: %v: %s", key, err.Error())
// 		}

// 		value, err := smartBucket.ValueCodec.Unmarshal(v)

// 		if err != nil {
// 			return fmt.Errorf("Could not unmarshal value: %v: %s", v, err.Error())
// 		}

// 		return fn(key, value)
// 	})
// }

// func (smartBucket *SmartBucket) Delete(key interface{}) error {
// 	marshaledKey, err := smartBucket.KeyCodec.Marshal(key)

// 	if err != nil {
// 		return fmt.Errorf("Could not marshal key: %s", err.Error())
// 	}

// 	err = smartBucket.Bucket.Delete(marshaledKey)

// 	if err != nil {
// 		return fmt.Errorf("Could not delete key: %v: %s", marshaledKey, err.Error())
// 	}

// 	return nil
// }

// func (smartBucket *SmartBucket) Get(key interface{}) (interface{}, error) {
// 	marshaledKey, err := smartBucket.KeyCodec.Marshal(key)

// 	if err != nil {
// 		return nil, fmt.Errorf("Could not marshal key: %s", err.Error())
// 	}

// 	marshaledValue := smartBucket.Bucket.Get(marshaledKey)

// 	value, err := smartBucket.ValueCodec.Unmarshal(marshaledValue)

// 	if err != nil {
// 		return nil, fmt.Errorf("Could not unmarshal value: %v: %s", marshaledValue, err.Error())
// 	}

// 	return value, nil
// }

// func (smartBucket *SmartBucket) Put(key interface{}, value interface{}) error {
// 	marshaledKey, err := smartBucket.KeyCodec.Marshal(key)

// 	if err != nil {
// 		return fmt.Errorf("Could not marshal key: %s", err.Error())
// 	}

// 	marshaledValue, err := smartBucket.ValueCodec.Marshal(value)

// 	if err != nil {
// 		return fmt.Errorf("Could not marshal value: %s", err.Error())
// 	}

// 	err = smartBucket.Bucket.Put(marshaledKey, marshaledValue)

// 	if err != nil {
// 		return fmt.Errorf("Could not put key=%v, value=%v: %s", marshaledKey, marshaledValue, err.Error())
// 	}

// 	return nil
// }

// func (smartBucket *SmartBucket) Cursor() *SmartCursor {

// 	return &SmartCursor{
// 		Cursor:     smartBucket.Bucket.Cursor(),
// 		KeyCodec:   smartBucket.KeyCodec,
// 		ValueCodec: smartBucket.ValueCodec,
// 	}
// }

// type SmartCursor struct {
// 	Cursor     Cursor
// 	KeyCodec   SmartBucketCodec
// 	ValueCodec SmartBucketCodec
// }

// func (smartCursor *SmartCursor) Delete() error {
// 	return smartCursor.Cursor.Delete()
// }

// func (smartCursor *SmartCursor) move(move func() ([]byte, []byte)) (interface{}, interface{}, error) {
// 	k, v := move()

// 	key, err := smartCursor.KeyCodec.Unmarshal(k)

// 	if err != nil {
// 		return nil, nil, fmt.Errorf("Could not unmarshal key %v: %s", k, err.Error())
// 	}

// 	value, err := smartCursor.ValueCodec.Unmarshal(v)

// 	if err != nil {
// 		return nil, nil, fmt.Errorf("Could not unmarshal value %v: %s", v, err.Error())
// 	}

// 	return key, value, nil
// }

// func (smartCursor *SmartCursor) First() (interface{}, interface{}, error) {
// 	return smartCursor.move(smartCursor.Cursor.First)
// }

// func (smartCursor *SmartCursor) Last() (interface{}, interface{}, error) {
// 	return smartCursor.move(smartCursor.Cursor.Last)
// }

// func (smartCursor *SmartCursor) Next() (interface{}, interface{}, error) {
// 	return smartCursor.move(smartCursor.Cursor.Next)
// }

// func (smartCursor *SmartCursor) Prev() (interface{}, interface{}, error) {
// 	return smartCursor.move(smartCursor.Cursor.Prev)
// }

// func (smartCursor *SmartCursor) Seek(seek interface{}) (interface{}, interface{}, error) {
// 	key, err := smartCursor.KeyCodec.Marshal(seek)

// 	if err != nil {
// 		return nil, nil, fmt.Errorf("Could not marshal key: %s", err.Error())
// 	}

// 	return smartCursor.move(func() ([]byte, []byte) { return smartCursor.Cursor.Seek(key) })
// }

// type SubStore struct {
// 	Store              Store
// 	BucketAddress      [][]byte
// 	SmartBucketBuilder func(bucket Bucket) *SmartBucket
// }

// func (subStore *SubStore) bucket(transaction Transaction) Bucket {
// 	var bucket Bucket

// 	if len(subStore.BucketAddress) == 0 {
// 		panic("Using SubStore with empty bucket address")
// 	}

// 	for _, b := range subStore.BucketAddress {
// 		bucket = transaction.Bucket(b)
// 	}

// 	return bucket
// }

// func (subStore *SubStore) smartBucket(transaction Transaction) *SmartBucket {
// 	return subStore.SmartBucketBuilder(subStore.bucket(transaction))
// }

// func (subStore *SubStore) ForEach(fn func(key interface{}, value interface{}) error) error {
// 	transaction, err := subStore.Store.Begin(false)

// 	if err != nil {
// 		return fmt.Errorf("Could not begin transaction: %s", err.Error())
// 	}

// 	defer transaction.Rollback()

// 	return subStore.smartBucket(transaction).ForEach(fn)
// }

// func (subStore *SubStore) Delete(key interface{}) error {
// 	transaction, err := subStore.Store.Begin(true)

// 	if err != nil {
// 		return fmt.Errorf("Could not begin transaction: %s", err.Error())
// 	}

// 	defer transaction.Rollback()

// 	err = subStore.smartBucket(transaction).Delete(key)

// 	if err != nil {
// 		return fmt.Errorf("Could not delete key: %v: %s", key, err.Error())
// 	}

// 	err = transaction.Commit()

// 	if err != nil {
// 		return fmt.Errorf("Could not commit transaction: %s", err.Error())
// 	}

// 	return nil
// }

// func (subStore *SubStore) Get(key interface{}) (interface{}, error) {
// 	transaction, err := subStore.Store.Begin(false)

// 	if err != nil {
// 		return nil, fmt.Errorf("Could not begin transaction: %s", err.Error())
// 	}

// 	defer transaction.Rollback()

// 	value, err := subStore.smartBucket(transaction).Get(key)

// 	if err != nil {
// 		return nil, fmt.Errorf("Could not get key: %v: %s", key, err.Error())
// 	}

// 	return value, nil
// }

// func (subStore *SubStore) Put(key interface{}, value interface{}) error {
// 	transaction, err := subStore.Store.Begin(true)

// 	if err != nil {
// 		return fmt.Errorf("Could not begin transaction: %s", err.Error())
// 	}

// 	defer transaction.Rollback()

// 	err = subStore.smartBucket(transaction).Put(key, value)

// 	if err != nil {
// 		return fmt.Errorf("Could not put key: %v: %s", key, err.Error())
// 	}

// 	err = transaction.Commit()

// 	if err != nil {
// 		return fmt.Errorf("Could not commit transaction: %s", err.Error())
// 	}

// 	return nil
// }
