package kv_test

import (
	"sort"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/plugins"
)

type rootStoreModel map[string]storeModel
type storeModel map[string]partitionModel
type partitionModel map[string]string

type tempStoreBuilder func(t *testing.T, initialState rootStoreModel) kv.RootStore

func builder(plugin kv.Plugin) tempStoreBuilder {
	return func(t *testing.T, initialState rootStoreModel) kv.RootStore {
		rootStore, err := plugin.NewTempRootStore()

		if err != nil {
			t.Fatalf("could not create temp root store: %s", err.Error())
		}

		if err := initializeRootStoreState(rootStore, initialState); err != nil {
			t.Fatalf("could not initialize root store state: %s", err.Error())
		}

		return rootStore
	}
}

func initializeRootStoreState(rootStore kv.RootStore, initialState rootStoreModel) error {
	for store, storeModel := range initialState {
		if err := rootStore.Store([]byte(store)).Create(); err != nil {
			return err
		}

		if err := initializeStoreState(rootStore.Store([]byte(store)), storeModel); err != nil {
			return err
		}
	}

	return nil
}

func initializeStoreState(store kv.Store, initialState storeModel) error {
	for partition, partitionModel := range initialState {
		if err := store.Partition([]byte(partition)).Create(); err != nil {
			return err
		}

		if err := initializePartitionState(store.Partition([]byte(partition)), partitionModel); err != nil {
			return err
		}
	}

	return nil
}

func initializePartitionState(partition kv.Partition, initialState partitionModel) error {
	transaction, err := partition.Begin(true)

	if err != nil {
		return err
	}

	defer transaction.Rollback()

	for key, value := range initialState {
		if err := transaction.Put([]byte(key), []byte(value)); err != nil {
			return err
		}
	}

	return transaction.Commit()
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
	t.Run("RootStore", func(t *testing.T) { testRootStore(builder, t) })
	t.Run("Store", func(t *testing.T) { testStore(builder, t) })
	t.Run("Partition", func(t *testing.T) { testPartition(builder, t) })
	t.Run("Transaction", func(t *testing.T) { testTransaction(builder, t) })
	t.Run("Iterator", func(t *testing.T) { testIterator(builder, t) })
}

func testRootStore(builder tempStoreBuilder, t *testing.T) {
	t.Run("Delete", func(t *testing.T) { testRootStoreDelete(builder, t) })
	t.Run("Close", func(t *testing.T) { testRootStoreClose(builder, t) })
	t.Run("Stores", func(t *testing.T) { testRootStoreStores(builder, t) })
	t.Run("Store", func(t *testing.T) { testRootStoreStore(builder, t) })
}

func testStore(builder tempStoreBuilder, t *testing.T) {
	t.Run("Name", func(t *testing.T) { testStoreName(builder, t) })
	t.Run("Create", func(t *testing.T) { testStoreCreate(builder, t) })
	t.Run("Delete", func(t *testing.T) { testStoreDelete(builder, t) })
	t.Run("Partitions", func(t *testing.T) { testStorePartitions(builder, t) })
	t.Run("Partition", func(t *testing.T) { testStorePartition(builder, t) })
}

func testPartition(builder tempStoreBuilder, t *testing.T) {
	t.Run("Name", func(t *testing.T) { testPartitionName(builder, t) })
	t.Run("Create", func(t *testing.T) { testPartitionCreate(builder, t) })
	t.Run("Delete", func(t *testing.T) { testPartitionDelete(builder, t) })
	t.Run("Begin", func(t *testing.T) { testPartitionBegin(builder, t) })
	t.Run("Snapshot", func(t *testing.T) { testPartitionSnapshot(builder, t) })
	t.Run("ApplySnapshot", func(t *testing.T) { testPartitionApplySnapshot(builder, t) })
}

func testTransaction(builder tempStoreBuilder, t *testing.T) {
	t.Run("Put", func(t *testing.T) { testTransactionPut(builder, t) })
	t.Run("Get", func(t *testing.T) { testTransactionGet(builder, t) })
	t.Run("Delete", func(t *testing.T) { testTransactionDelete(builder, t) })
	t.Run("Keys", func(t *testing.T) { testTransactionKeys(builder, t) })
	t.Run("Commit", func(t *testing.T) { testTransactionCommit(builder, t) })
	t.Run("Rollback", func(t *testing.T) { testTransactionRollback(builder, t) })
}

func testIterator(builder tempStoreBuilder, t *testing.T) {
	t.Run("Next", func(t *testing.T) {})
	t.Run("Key", func(t *testing.T) {})
	t.Run("Value", func(t *testing.T) {})
	t.Run("Error", func(t *testing.T) {})
}

// Parallel operations record "events" to the history
// at certain times. At the end we can compare the ordering
// of the history to the expected ordering.
type set struct {
	mu   sync.Mutex
	data map[string]bool
}

func (set *set) init() {
	if set.data == nil {
		return
	}

	set.data = map[string]bool{}
}

func (set *set) add(s string) {
	set.mu.Lock()
	defer set.mu.Unlock()

	set.init()
	set.data[s] = true
}

func (set *set) remove(s string) {
	set.mu.Lock()
	defer set.mu.Unlock()

	set.init()
	delete(set.data, s)
}

func (set *set) has(s string) bool {
	set.mu.Lock()
	defer set.mu.Unlock()

	set.init()
	_, ok := set.data[s]

	return ok
}

// RootStore
func testRootStoreDelete(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
	}{}

	for name, _ := range testCases {
		t.Run(name, func(t *testing.T) {
			// TODO: Test cases that validate ordering guarantees of the interface
			// such as ensuring that delete closes the store.
		})
	}
}

func testRootStoreClose(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
	}{}

	for name, _ := range testCases {
		t.Run(name, func(t *testing.T) {
			// TODO: Test cases that validate ordering guarantees of the interface
			// such as ensuring that close waits until concurrent transactions have
			// completed to return.
		})
	}
}

func testRootStoreStores(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
	}{
		"empty-root-store": {
			initialState: rootStoreModel{},
		},
		"several-empty-stores": {
			initialState: rootStoreModel{
				"store1": storeModel{},
				"store2": storeModel{},
				"store3": storeModel{},
			},
		},
		"several-stores-with-data": {
			initialState: rootStoreModel{
				"store1": storeModel{
					"p1": partitionModel{
						"a": "b",
					},
					"p2": partitionModel{},
				},
				"store2": storeModel{
					"p1": partitionModel{
						"a": "b",
					},
					"p2": partitionModel{
						"c": "d",
					},
				},
				"store3": storeModel{},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()
			expectedStores := []string{}

			for store, _ := range testCase.initialState {
				expectedStores = append(expectedStores, store)
			}

			stores, err := rootStore.Stores()

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			actualStores := []string{}

			for _, store := range stores {
				actualStores = append(actualStores, string(store))
			}

			sort.Strings(expectedStores)
			sort.Strings(actualStores)

			diff := cmp.Diff(expectedStores, actualStores)

			if diff != "" {
				t.Fatalf(diff)
			}

			if err := rootStore.Close(); err != nil {
				t.Errorf("expected err to be nil, got %#v", err)
			}

			_, err = rootStore.Stores()

			if err != kv.ErrClosed {
				t.Errorf("expected err to be %#v, got %#v", kv.ErrClosed, err)
			}
		})
	}
}

func testRootStoreStore(builder tempStoreBuilder, t *testing.T) {
}

// Store
func testStoreName(builder tempStoreBuilder, t *testing.T) {
}

func testStoreCreate(builder tempStoreBuilder, t *testing.T) {
}

func testStoreDelete(builder tempStoreBuilder, t *testing.T) {
}

func testStorePartitions(builder tempStoreBuilder, t *testing.T) {
}

func testStorePartition(builder tempStoreBuilder, t *testing.T) {
}

// Partition
func testPartitionName(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionCreate(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionDelete(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionBegin(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionSnapshot(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionApplySnapshot(builder tempStoreBuilder, t *testing.T) {
}

// Transaction
func testTransactionPut(builder tempStoreBuilder, t *testing.T) {
}

func testTransactionGet(builder tempStoreBuilder, t *testing.T) {
}

func testTransactionDelete(builder tempStoreBuilder, t *testing.T) {
}

func testTransactionKeys(builder tempStoreBuilder, t *testing.T) {
}

func testTransactionCommit(builder tempStoreBuilder, t *testing.T) {
}

func testTransactionRollback(builder tempStoreBuilder, t *testing.T) {
}
