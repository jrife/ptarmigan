package kv_test

import (
	"fmt"
	"testing"

	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/plugins"
)

type storeModel bucketModel
type bucketModel map[string]interface{}

func writeStore(store kv.SubStore, model storeModel) error {
	transaction, err := store.Begin(true)

	if err != nil {
		return err
	}

	defer transaction.Rollback()

	if err := writeBucket(transaction.Root(), (bucketModel)(model)); err != nil {
		return err
	}

	if err := transaction.Commit(); err != nil {
		return err
	}

	return nil
}

func writeBucket(bucket kv.Bucket, model bucketModel) error {
	for key, value := range model {
		if innerBucket, ok := value.(bucketModel); ok {
			b, err := bucket.CreateBucket([]byte(key))

			if err != nil {
				return err
			}

			if err := writeBucket(b, innerBucket); err != nil {
				return err
			}
		} else if byteValue, ok := value.([]byte); ok {
			if err := bucket.Put([]byte(key), byteValue); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("Value in model was neither a byte slice nor a bucketModel")
		}
	}

	return nil
}

func readStore(store kv.SubStore) (storeModel, error) {
	transaction, err := store.Begin(false)

	if err != nil {
		return nil, err
	}

	defer transaction.Rollback()

	sm := storeModel{}

	cursor := transaction.Root().Cursor()

	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		if value == nil {
			bucket := transaction.Root().Bucket(key)
			bm, err := readBucket(bucket)

			if err != nil {
				return nil, err
			}

			sm[string(key)] = bm
		} else {
			sm[string(key)] = value
		}
	}

	return sm, nil
}

func readBucket(bucket kv.Bucket) (bucketModel, error) {
	cursor := bucket.Cursor()

	bm := bucketModel{}

	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		if value == nil {
			innerBucket, err := readBucket(bucket.Bucket(key))

			if err != nil {
				return nil, err
			}

			bm[string(key)] = innerBucket
		} else {
			bm[string(key)] = value
		}
	}

	return bm, nil
}

type tempStoreBuilder func(t *testing.T, model storeModel) kv.Store

func builder(plugin kv.Plugin) tempStoreBuilder {
	return func(t *testing.T, model storeModel) kv.Store {
		store, err := plugin.NewTempStore()

		if err != nil {
			t.Fatalf("Could not build a %s store: %s", plugin.Name(), err.Error())
		}

		if model != nil {
			if err := writeStore(store, model); err != nil {
				t.Fatalf("Could not populate %s store: %s", plugin.Name(), err.Error())
			}
		}

		return store
	}
}

func TestDrivers(t *testing.T) {
	pluginManager := plugins.NewKVPluginManager()

	for _, plugin := range pluginManager.Plugins() {
		t.Run(plugin.Name(), driverTest(builder(plugin)))
	}
}

func driverTest(builder tempStoreBuilder) func(t *testing.T) {
	return func(t *testing.T) {
		testDriver(builder, t)
	}
}

func testDriver(builder tempStoreBuilder, t *testing.T) {
	// Snapshots
	t.Run("snapshots", func(t *testing.T) { testSnapshots(builder, t) })
}
