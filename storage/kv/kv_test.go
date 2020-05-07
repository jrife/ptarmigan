package kv_test

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/plugins"
)

var errAny = errors.New("any error")

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
		if err := store.Partition([]byte(partition)).Create([]byte{}); err != nil {
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

func rootStoreToModel(rootStore kv.RootStore) (rootStoreModel, error) {
	model := rootStoreModel{}
	stores, err := rootStore.Stores()

	if err != nil {
		return rootStoreModel{}, fmt.Errorf("could not list stores: %s", err.Error())
	}

	for _, storeName := range stores {
		storeModel, err := storeToModel(rootStore.Store(storeName))

		if err != nil {
			return rootStoreModel{}, fmt.Errorf("storeToModel failed for store %s: %s", storeName, err.Error())
		}

		model[string(storeName)] = storeModel
	}

	return model, nil
}

func storeToModel(store kv.Store) (storeModel, error) {
	model := storeModel{}
	partitions, err := store.Partitions(nil, nil, -1)

	if err != nil {
		return storeModel{}, fmt.Errorf("could not list partitions: %s", err.Error())
	}

	for _, partitionName := range partitions {
		partitionModel, err := partitionToModel(store.Partition(partitionName))

		if err != nil {
			return storeModel{}, fmt.Errorf("partitionToModel failed for partition %s: %s", partitionName, err.Error())
		}

		model[string(partitionName)] = partitionModel
	}

	return model, nil
}

func partitionToModel(partition kv.Partition) (partitionModel, error) {
	model := partitionModel{}
	transaction, err := partition.Begin(false)

	if err != nil {
		return partitionModel{}, fmt.Errorf("could not begin transaction: %s", err.Error())
	}

	defer transaction.Rollback()

	iter, err := transaction.Keys(nil, nil, kv.SortOrderAsc)

	if err != nil {
		return partitionModel{}, fmt.Errorf("could not create key iterator: %s", err.Error())
	}

	for iter.Next() {
		model[string(iter.Key())] = string(iter.Value())
	}

	if iter.Error() != nil {
		return partitionModel{}, fmt.Errorf("iteration error: %s", err.Error())
	}

	return model, nil
}

func TestDrivers(t *testing.T) {
	for _, plugin := range plugins.Plugins() {
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
	t.Run("Read-Write", func(t *testing.T) { testTransactionReadWrite(builder, t) })
	t.Run("Keys", func(t *testing.T) { testTransactionKeys(builder, t) })
	t.Run("Namespace", func(t *testing.T) { testTransactionNamespace(builder, t) })
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

	for name := range testCases {
		t.Run(name, func(t *testing.T) {
			// TODO: Test cases that validate ordering guarantees of the interface
			// such as ensuring that delete closes the store.
		})
	}
}

func testRootStoreClose(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
	}{}

	for name := range testCases {
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
				"store1": {},
				"store2": {},
				"store3": {},
			},
		},
		"several-stores-with-data": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
					},
					"p2": {},
				},
				"store2": {
					"p1": {
						"a": "b",
					},
					"p2": {
						"c": "d",
					},
				},
				"store3": {},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()
			expectedStores := []string{}

			for store := range testCase.initialState {
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
	testCases := map[string]struct{}{}

	for name := range testCases {
		t.Run(name, func(t *testing.T) {
			// TODO: Maybe just some test cases to ensure that the Store
			// function doesn't return nil under several cases
		})
	}
}

// Store
func testStoreName(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		name         []byte
	}{
		"store-that-doesn't-exist": {
			initialState: rootStoreModel{},
			name:         []byte("a"),
		},
		"store-that-exists": {
			initialState: rootStoreModel{
				"store1": {},
				"store2": {},
				"store3": {},
			},
			name: []byte("store2"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			if bytes.Compare(rootStore.Store(testCase.name).Name(), testCase.name) != 0 {
				t.Errorf("expected name %#v, got #%v", testCase.name, rootStore.Store(testCase.name).Name())
			}
		})
	}
}

func testStoreCreate(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		finalState   rootStoreModel
		name         []byte
		err          error
	}{
		"new-store": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {},
				},
				"store2": {},
			},
			name: []byte("store2"),
		},
		"existing-store": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			name: []byte("store2"),
		},
		"nil-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			name: nil,
			err:  errAny,
		},
		"empty-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			name: []byte{},
			err:  errAny,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			err := rootStore.Store(testCase.name).Create()

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected err to be %#v, got %#v", testCase.err, err)
			}

			finalState, err := rootStoreToModel(rootStore)

			if err != nil {
				t.Fatalf("expected err to be nil, got #%v", err)
			}

			diff := cmp.Diff(testCase.finalState, finalState)

			if diff != "" {
				t.Fatalf(diff)
			}

			if err := rootStore.Close(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			if err := rootStore.Store(testCase.name).Create(); err != kv.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", kv.ErrClosed, err)
			}
		})
	}
}

func testStoreDelete(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		finalState   rootStoreModel
		err          error
		name         []byte
	}{
		"not-existing-store": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
				"store2": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {},
				},
				"store2": {},
			},
			name: []byte("store3"),
		},
		"existing-store": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store3": {},
			},
			name: []byte("store2"),
		},
		"nil-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			name: nil,
			err:  errAny,
		},
		"empty-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			name: []byte{},
			err:  errAny,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			err := rootStore.Store(testCase.name).Delete()

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected err to be %#v, got %#v", testCase.err, err)
			}

			finalState, err := rootStoreToModel(rootStore)

			if err != nil {
				t.Fatalf("expected err to be nil, got #%v", err)
			}

			diff := cmp.Diff(testCase.finalState, finalState)

			if diff != "" {
				t.Fatalf(diff)
			}

			if err := rootStore.Close(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			if err := rootStore.Store(testCase.name).Delete(); err != kv.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", kv.ErrClosed, err)
			}
		})
	}
}

func testStorePartitions(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		partitions   [][]byte
		err          error
		store        []byte
		min          []byte
		max          []byte
		limit        int
	}{
		"all-partitions": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store: []byte("store1"),
			min:   nil,
			max:   nil,
			limit: -1,
			err:   nil,
			partitions: [][]byte{
				[]byte("p1"),
				[]byte("p2"),
				[]byte("p3"),
				[]byte("p4"),
				[]byte("p5"),
				[]byte("p6"),
			},
		},
		"some-partitions-limit-only": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store: []byte("store1"),
			min:   nil,
			max:   nil,
			limit: 3,
			err:   nil,
			partitions: [][]byte{
				[]byte("p1"),
				[]byte("p2"),
				[]byte("p3"),
			},
		},
		"some-partitions-middle-to-end": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store: []byte("store1"),
			min:   []byte("p4"),
			max:   nil,
			limit: -1,
			err:   nil,
			partitions: [][]byte{
				[]byte("p4"),
				[]byte("p5"),
				[]byte("p6"),
			},
		},
		"some-partitions-start-to-middle": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store: []byte("store1"),
			min:   nil,
			max:   []byte("p4"),
			limit: -1,
			err:   nil,
			partitions: [][]byte{
				[]byte("p1"),
				[]byte("p2"),
				[]byte("p3"),
			},
		},
		"some-partitions-middle-to-middle": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store: []byte("store1"),
			min:   []byte("p2"),
			max:   []byte("p4"),
			limit: -1,
			err:   nil,
			partitions: [][]byte{
				[]byte("p2"),
				[]byte("p3"),
			},
		},
		"store-that-doesn't-exist": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store:      []byte("store2"),
			min:        nil,
			max:        nil,
			limit:      -1,
			err:        kv.ErrNoSuchStore,
			partitions: nil,
		},
		"min=max": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store:      []byte("store1"),
			min:        []byte("p3"),
			max:        []byte("p3"),
			limit:      -1,
			err:        nil,
			partitions: [][]byte{},
		},
		"min>max": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store:      []byte("store1"),
			min:        []byte("p4"),
			max:        []byte("p2"),
			limit:      -1,
			err:        nil,
			partitions: [][]byte{},
		},
		"nil-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store:      nil,
			min:        []byte("p3"),
			max:        []byte("p3"),
			limit:      -1,
			err:        errAny,
			partitions: [][]byte{},
		},
		"empty-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
					"p3": {},
					"p4": {},
					"p5": {},
					"p6": {},
				},
			},
			store:      []byte{},
			min:        []byte("p3"),
			max:        []byte("p3"),
			limit:      -1,
			err:        errAny,
			partitions: [][]byte{},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			partitions, err := rootStore.Store(testCase.store).Partitions(testCase.min, testCase.max, testCase.limit)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected err to be %#v, got %#v", testCase.err, err)
			}

			if err == nil {
				diff := cmp.Diff(testCase.partitions, partitions)

				if diff != "" {
					t.Fatalf(diff)
				}
			}

			if err := rootStore.Close(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			if _, err := rootStore.Store(testCase.store).Partitions(testCase.min, testCase.max, testCase.limit); err != kv.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", kv.ErrClosed, err)
			}
		})
	}
}

func testStorePartition(builder tempStoreBuilder, t *testing.T) {
	// TODO: maybe some test to make sure Partition() doesn't return nil
}

// Partition
func testPartitionName(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		store        []byte
		name         []byte
	}{
		"store-that-doesn't-exist": {
			initialState: rootStoreModel{},
			store:        []byte("store1"),
			name:         []byte("p1"),
		},
		"partition-that-doesn't-exist": {
			initialState: rootStoreModel{
				"store1": {},
			},
			store: []byte("store1"),
			name:  []byte("p1"),
		},
		"store-and-partition-exist": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			store: []byte("store1"),
			name:  []byte("p1"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			if bytes.Compare(rootStore.Store(testCase.store).Partition(testCase.name).Name(), testCase.name) != 0 {
				t.Errorf("expected name %#v, got #%v", testCase.name, rootStore.Store(testCase.store).Partition(testCase.name).Name())
			}
		})
	}
}

func testPartitionCreate(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		finalState   rootStoreModel
		err          error
		store        []byte
		name         []byte
		metadata     []byte
	}{
		"new-partition": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {},
					"p2": {},
				},
			},
			store:    []byte("store1"),
			name:     []byte("p2"),
			metadata: []byte("ok"),
		},
		"existing-partition": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store:    []byte("store2"),
			name:     []byte("p1"),
			metadata: []byte("aaa"),
		},
		"store-doesn't-exist": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store:    []byte("store4"),
			name:     []byte("p1"),
			metadata: []byte("ok"),
			err:      kv.ErrNoSuchStore,
		},
		"nil-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store:    nil,
			name:     []byte("p1"),
			metadata: []byte("ok"),
			err:      errAny,
		},
		"empty-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store:    []byte{},
			name:     []byte("p1"),
			metadata: []byte("ok"),
			err:      errAny,
		},
		"nil-partition-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store:    []byte("store1"),
			name:     nil,
			metadata: []byte("ok"),
			err:      errAny,
		},
		"empty-partition-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store:    []byte("store1"),
			name:     []byte{},
			metadata: []byte("ok"),
			err:      errAny,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			err := rootStore.Store(testCase.store).Partition(testCase.name).Create(testCase.metadata)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected err to be #%v, got %#v", testCase.err, err)
			}

			finalState, err := rootStoreToModel(rootStore)

			if err != nil {
				t.Fatalf("expected err to be nil, got #%v", err)
			}

			diff := cmp.Diff(testCase.finalState, finalState)

			if diff != "" {
				t.Fatalf(diff)
			}

			if err := rootStore.Close(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			if err := rootStore.Store(testCase.store).Partition(testCase.name).Create(testCase.metadata); err != kv.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", kv.ErrClosed, err)
			}
		})
	}
}

func testPartitionDelete(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		finalState   rootStoreModel
		err          error
		store        []byte
		name         []byte
	}{
		"not-existing-partition": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			store: []byte("store1"),
			name:  []byte("p2"),
		},
		"existing-partition": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store2"),
			name:  []byte("p1"),
		},
		"store-doesn't-exist": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store4"),
			name:  []byte("p1"),
			err:   kv.ErrNoSuchStore,
		},
		"nil-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: nil,
			name:  []byte("p1"),
			err:   errAny,
		},
		"empty-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte{},
			name:  []byte("p1"),
			err:   errAny,
		},
		"nil-partition-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store1"),
			name:  nil,
			err:   errAny,
		},
		"empty-partition-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			finalState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store1"),
			name:  []byte{},
			err:   errAny,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			err := rootStore.Store(testCase.store).Partition(testCase.name).Delete()

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected err to be #%v, got %#v", testCase.err, err)
			}

			finalState, err := rootStoreToModel(rootStore)

			if err != nil {
				t.Fatalf("expected err to be nil, got #%v", err)
			}

			diff := cmp.Diff(testCase.finalState, finalState)

			if diff != "" {
				t.Fatalf(diff)
			}

			if err := rootStore.Close(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			if err := rootStore.Store(testCase.store).Partition(testCase.name).Delete(); err != kv.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", kv.ErrClosed, err)
			}
		})
	}
}

func testPartitionBegin(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		err          error
		writable     bool
		store        []byte
		name         []byte
	}{
		"not-existing-partition": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			store: []byte("store1"),
			name:  []byte("p2"),
			err:   kv.ErrNoSuchPartition,
		},
		"existing-partition": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store2"),
			name:  []byte("p1"),
		},
		"store-doesn't-exist": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store4"),
			name:  []byte("p1"),
			err:   kv.ErrNoSuchStore,
		},
		"nil-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: nil,
			name:  []byte("p1"),
			err:   errAny,
		},
		"empty-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte{},
			name:  []byte("p1"),
			err:   errAny,
		},
		"nil-partition-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store1"),
			name:  nil,
			err:   errAny,
		},
		"empty-partition-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store1"),
			name:  []byte{},
			err:   errAny,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			transaction, err := rootStore.Store(testCase.store).Partition(testCase.name).Begin(testCase.writable)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected err to be #%v, got %#v", testCase.err, err)
			}

			if err == nil {
				if err := transaction.Rollback(); err != nil {
					t.Fatalf("expected err to be nil, got #%v", err)
				}
			}

			if err := rootStore.Close(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			if _, err := rootStore.Store(testCase.store).Partition(testCase.name).Begin(testCase.writable); err != kv.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", kv.ErrClosed, err)
			}
		})
	}
}

func testPartitionSnapshot(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		err          error
		store        []byte
		name         []byte
	}{
		"not-existing-partition": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			store: []byte("store1"),
			name:  []byte("p2"),
			err:   kv.ErrNoSuchPartition,
		},
		"existing-partition": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store2"),
			name:  []byte("p1"),
		},
		"store-doesn't-exist": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store4"),
			name:  []byte("p1"),
			err:   kv.ErrNoSuchStore,
		},
		"nil-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: nil,
			name:  []byte("p1"),
			err:   errAny,
		},
		"empty-store-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte{},
			name:  []byte("p1"),
			err:   errAny,
		},
		"nil-partition-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store1"),
			name:  nil,
			err:   errAny,
		},
		"empty-partition-name": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {"a": "b"},
				},
				"store2": {
					"p1": {"x": "y"},
					"p2": {"c": "d"},
				},
				"store3": {},
			},
			store: []byte("store1"),
			name:  []byte{},
			err:   errAny,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			snap, err := rootStore.Store(testCase.store).Partition(testCase.name).Snapshot()

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected err to be #%v, got %#v", testCase.err, err)
			}

			if err == nil {
				if err := snap.Close(); err != nil {
					t.Fatalf("expected err to be nil, got #%v", err)
				}
			}

			if err := rootStore.Close(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			if _, err := rootStore.Store(testCase.store).Partition(testCase.name).Snapshot(); err != kv.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", kv.ErrClosed, err)
			}
		})
	}
}

func testPartitionApplySnapshot(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialSourceState rootStoreModel
		initialDestState   rootStoreModel
		finalDestState     rootStoreModel
		sourceStore        []byte
		destStore          []byte
		sourcePartition    []byte
		destPartition      []byte
		err                error
		store              []byte
		name               []byte
	}{
		"store-does-not-exist-at-dest": {
			initialSourceState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			initialDestState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			finalDestState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			sourceStore:     []byte("store1"),
			destStore:       []byte("store2"),
			sourcePartition: []byte("p1"),
			destPartition:   []byte("p1"),
			err:             kv.ErrNoSuchStore,
		},
		"create-new-empty-partition-at-dest-same-store-name": {
			initialSourceState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			initialDestState: rootStoreModel{
				"store1": {},
			},
			finalDestState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			sourceStore:     []byte("store1"),
			destStore:       []byte("store1"),
			sourcePartition: []byte("p1"),
			destPartition:   []byte("p1"),
			err:             nil,
		},
		"create-new-empty-partition-at-dest-different-store-name": {
			initialSourceState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			initialDestState: rootStoreModel{
				"store2": {},
			},
			finalDestState: rootStoreModel{
				"store2": {
					"p1": {},
				},
			},
			sourceStore:     []byte("store1"),
			destStore:       []byte("store2"),
			sourcePartition: []byte("p1"),
			destPartition:   []byte("p1"),
			err:             nil,
		},
		"create-new-empty-partition-at-dest-different-partition-name": {
			initialSourceState: rootStoreModel{
				"store1": {
					"p1": {},
				},
			},
			initialDestState: rootStoreModel{
				"store2": {},
			},
			finalDestState: rootStoreModel{
				"store2": {
					"p2": {},
				},
			},
			sourceStore:     []byte("store1"),
			destStore:       []byte("store2"),
			sourcePartition: []byte("p1"),
			destPartition:   []byte("p2"),
			err:             nil,
		},
		"overwrite-keys-at-dest": {
			initialSourceState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "1",
						"2": "3",
						"4": "5",
					},
				},
			},
			initialDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
					},
				},
			},
			finalDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "1",
						"2": "3",
						"4": "5",
					},
				},
			},
			sourceStore:     []byte("store1"),
			destStore:       []byte("store2"),
			sourcePartition: []byte("p1"),
			destPartition:   []byte("p1"),
			err:             nil,
		},
		"nil-store-name": {
			initialSourceState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "1",
						"2": "3",
						"4": "5",
					},
				},
			},
			initialDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
					},
				},
			},
			finalDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
					},
				},
			},
			sourceStore:     []byte("store1"),
			destStore:       nil,
			sourcePartition: []byte("p1"),
			destPartition:   []byte("p1"),
			err:             errAny,
		},
		"empty-store-name": {
			initialSourceState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "1",
						"2": "3",
						"4": "5",
					},
				},
			},
			initialDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
					},
				},
			},
			finalDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
					},
				},
			},
			sourceStore:     []byte("store1"),
			destStore:       []byte{},
			sourcePartition: []byte("p1"),
			destPartition:   []byte("p1"),
			err:             errAny,
		},
		"nil-partition-name": {
			initialSourceState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "1",
						"2": "3",
						"4": "5",
					},
				},
			},
			initialDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
					},
				},
			},
			finalDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
					},
				},
			},
			sourceStore:     []byte("store1"),
			destStore:       []byte("store2"),
			sourcePartition: []byte("p1"),
			destPartition:   nil,
			err:             errAny,
		},
		"empty-partition-name": {
			initialSourceState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "1",
						"2": "3",
						"4": "5",
					},
				},
			},
			initialDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
					},
				},
			},
			finalDestState: rootStoreModel{
				"store2": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
					},
				},
			},
			sourceStore:     []byte("store1"),
			destStore:       []byte("store2"),
			sourcePartition: []byte("p1"),
			destPartition:   []byte{},
			err:             errAny,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStoreSource := builder(t, testCase.initialSourceState)
			rootStoreDest := builder(t, testCase.initialDestState)
			defer rootStoreSource.Delete()
			defer rootStoreDest.Delete()

			snap, err := rootStoreSource.Store(testCase.sourceStore).Partition(testCase.sourcePartition).Snapshot()

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			defer snap.Close()

			err = rootStoreDest.Store(testCase.destStore).Partition(testCase.destPartition).ApplySnapshot(snap)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected err to be #%v, got %#v", testCase.err, err)
			}

			finalState, err := rootStoreToModel(rootStoreDest)

			if err != nil {
				t.Fatalf("expected err to be nil, got #%v", err)
			}

			diff := cmp.Diff(testCase.finalDestState, finalState)

			if diff != "" {
				t.Fatalf(diff)
			}

			if err := rootStoreDest.Close(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			if err := rootStoreDest.Store(testCase.destStore).Partition(testCase.destPartition).ApplySnapshot(snap); err != kv.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", kv.ErrClosed, err)
			}
		})
	}
}

func getKeys(txn kv.Transaction, keys [][]byte) [][]byte {
	values := make([][]byte, len(keys))

	for i, key := range keys {
		values[i], _ = txn.Get(key)
	}

	return values
}

func getSequence(iter kv.Iterator) [][2][]byte {
	kvs := [][2][]byte{}

	for iter.Next() {
		kvs = append(kvs, [2][]byte{
			iter.Key(),
			iter.Value(),
		})
	}

	return kvs
}

// Transaction
func testTransactionReadWrite(builder tempStoreBuilder, t *testing.T) {
	initialState := rootStoreModel{
		"store1": {
			"p1": {
				"a": "b",
				"c": "d",
			},
		},
	}
	rootStore := builder(t, initialState)
	defer rootStore.Delete()

	rw, err := rootStore.Store([]byte("store1")).Partition([]byte("p1")).Begin(true)

	if err != nil {
		t.Fatalf("expected err to be nil, got #%v", err)
	}

	defer rw.Rollback()

	ro, err := rootStore.Store([]byte("store1")).Partition([]byte("p1")).Begin(false)

	if err != nil {
		t.Fatalf("expected err to be nil, got #%v", err)
	}

	defer ro.Rollback()

	// Changes within the transaction should be observable to that transaction before commit
	// but not to other transactions
	if err := rw.Put([]byte("a"), []byte("z")); err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	if err := rw.Put([]byte("e"), []byte("f")); err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	if err := rw.Put(nil, []byte("f")); err == nil {
		t.Fatalf("expected err to not be nil, got nil")
	}

	if err := rw.Put([]byte{}, []byte("f")); err == nil {
		t.Fatalf("expected err to not be nil, got nil")
	}

	if err := rw.Put([]byte("a"), nil); err == nil {
		t.Fatalf("expected err to not be nil, got nil")
	}

	if err := rw.Delete(nil); err == nil {
		t.Fatalf("expected err to not be nil, got nil")
	}

	if err := rw.Delete([]byte{}); err == nil {
		t.Fatalf("expected err to not be nil, got nil")
	}

	diff := cmp.Diff([][]byte{[]byte("z"), []byte("d"), []byte("f")}, getKeys(rw, [][]byte{[]byte("a"), []byte("c"), []byte("e")}))

	if diff != "" {
		t.Fatalf(diff)
	}

	diff = cmp.Diff([][]byte{[]byte("b"), []byte("d"), nil}, getKeys(ro, [][]byte{[]byte("a"), []byte("c"), []byte("e")}))

	if diff != "" {
		t.Fatalf(diff)
	}

	rwIter, err := rw.Keys(nil, nil, kv.SortOrderAsc)

	if err != nil {
		t.Fatalf("expected err to be nil, got #%v", err)
	}

	roIter, err := ro.Keys(nil, nil, kv.SortOrderAsc)

	if err != nil {
		t.Fatalf("expected err to be nil, got #%v", err)
	}

	diff = cmp.Diff([][2][]byte{
		{[]byte("a"), []byte("z")},
		{[]byte("c"), []byte("d")},
		{[]byte("e"), []byte("f")},
	}, getSequence(rwIter))

	if diff != "" {
		t.Fatalf(diff)
	}

	diff = cmp.Diff([][2][]byte{
		{[]byte("a"), []byte("b")},
		{[]byte("c"), []byte("d")},
	}, getSequence(roIter))

	if diff != "" {
		t.Fatalf(diff)
	}
}

func testTransactionKeys(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState rootStoreModel
		store        []byte
		partition    []byte
		min          []byte
		max          []byte
		order        kv.SortOrder
		kvs          [][2][]byte
		err          error
	}{
		"all-keys-asc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       nil,
			max:       nil,
			order:     kv.SortOrderAsc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("a"), []byte("b")},
				{[]byte("c"), []byte("d")},
				{[]byte("e"), []byte("f")},
				{[]byte("g"), []byte("h")},
				{[]byte("i"), []byte("j")},
				{[]byte("k"), []byte("l")},
			},
		},
		"all-keys-desc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       nil,
			max:       nil,
			order:     kv.SortOrderDesc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("k"), []byte("l")},
				{[]byte("i"), []byte("j")},
				{[]byte("g"), []byte("h")},
				{[]byte("e"), []byte("f")},
				{[]byte("c"), []byte("d")},
				{[]byte("a"), []byte("b")},
			},
		},
		"bottom-half-keys-asc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       nil,
			max:       []byte("g"),
			order:     kv.SortOrderAsc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("a"), []byte("b")},
				{[]byte("c"), []byte("d")},
				{[]byte("e"), []byte("f")},
			},
		},
		"bottom-half-keys-desc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       nil,
			max:       []byte("g"),
			order:     kv.SortOrderDesc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("e"), []byte("f")},
				{[]byte("c"), []byte("d")},
				{[]byte("a"), []byte("b")},
			},
		},
		"top-half-keys-asc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("g"),
			max:       []byte("z"),
			order:     kv.SortOrderAsc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("g"), []byte("h")},
				{[]byte("i"), []byte("j")},
				{[]byte("k"), []byte("l")},
			},
		},
		"top-half-keys-desc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("g"),
			max:       []byte("z"),
			order:     kv.SortOrderDesc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("k"), []byte("l")},
				{[]byte("i"), []byte("j")},
				{[]byte("g"), []byte("h")},
			},
		},
		"middle-keys-asc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("c"),
			max:       []byte("i"),
			order:     kv.SortOrderAsc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("c"), []byte("d")},
				{[]byte("e"), []byte("f")},
				{[]byte("g"), []byte("h")},
			},
		},
		"middle-keys-desc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("c"),
			max:       []byte("i"),
			order:     kv.SortOrderDesc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("g"), []byte("h")},
				{[]byte("e"), []byte("f")},
				{[]byte("c"), []byte("d")},
			},
		},
		"sparse-keys-asc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("b"),
			max:       []byte("j"),
			order:     kv.SortOrderAsc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("c"), []byte("d")},
				{[]byte("e"), []byte("f")},
				{[]byte("g"), []byte("h")},
				{[]byte("i"), []byte("j")},
			},
		},
		"sparse-keys-desc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("b"),
			max:       []byte("j"),
			order:     kv.SortOrderDesc,
			err:       nil,
			kvs: [][2][]byte{
				{[]byte("i"), []byte("j")},
				{[]byte("g"), []byte("h")},
				{[]byte("e"), []byte("f")},
				{[]byte("c"), []byte("d")},
			},
		},
		"min=max asc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("e"),
			max:       []byte("e"),
			order:     kv.SortOrderAsc,
			err:       nil,
			kvs:       [][2][]byte{},
		},
		"min>max asc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("g"),
			max:       []byte("c"),
			order:     kv.SortOrderDesc,
			err:       nil,
			kvs:       [][2][]byte{},
		},
		"min=max desc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("e"),
			max:       []byte("e"),
			order:     kv.SortOrderDesc,
			err:       nil,
			kvs:       [][2][]byte{},
		},
		"min>max desc": {
			initialState: rootStoreModel{
				"store1": {
					"p1": {
						"a": "b",
						"c": "d",
						"e": "f",
						"g": "h",
						"i": "j",
						"k": "l",
					},
				},
			},
			store:     []byte("store1"),
			partition: []byte("p1"),
			min:       []byte("g"),
			max:       []byte("c"),
			order:     kv.SortOrderDesc,
			err:       nil,
			kvs:       [][2][]byte{},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			rootStore := builder(t, testCase.initialState)
			defer rootStore.Delete()

			transaction, err := rootStore.Store(testCase.store).Partition(testCase.partition).Begin(false)

			if err != testCase.err {
				t.Fatalf("expected err to be %#v, got #%v", testCase.err, err)
			}

			defer transaction.Rollback()

			iter, err := transaction.Keys(testCase.min, testCase.max, testCase.order)

			if err != testCase.err {
				t.Fatalf("expected error to be #%v, got %#v", testCase.err, err)
			} else if err != nil {
				return
			}

			diff := cmp.Diff(testCase.kvs, getSequence(iter))

			if iter.Error() != nil {
				t.Fatalf("expected error to be #%v, got #%v", testCase.err, iter.Error())
			}

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func testTransactionNamespace(builder tempStoreBuilder, t *testing.T) {
	initialState := rootStoreModel{
		"store1": {
			"p1": {
				"aaa123": "1",
				"aaa456": "2",
				"aaa789": "3",
				"bbb123": "4",
				"bbb456": "5",
				"bbb789": "6",
				"ccc123": "7",
				"ccc456": "8",
				"ccc789": "9",
			},
		},
	}
	rootStore := builder(t, initialState)
	defer rootStore.Delete()

	transaction, err := rootStore.Store([]byte("store1")).Partition([]byte("p1")).Begin(true)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	defer transaction.Rollback()

	aaaTxn := kv.Namespace(transaction, []byte("aaa"))
	bbbTxn := kv.Namespace(transaction, []byte("bbb"))
	cccTxn := kv.Namespace(transaction, []byte("ccc"))

	aaa123, err := aaaTxn.Get([]byte("123"))

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	if bytes.Compare(aaa123, []byte("1")) != 0 {
		t.Fatalf("expected #%v, got %#v", []byte("1"), aaa123)
	}

	bbb123, err := bbbTxn.Get([]byte("123"))

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	if bytes.Compare(bbb123, []byte("4")) != 0 {
		t.Fatalf("expected #%v, got %#v", []byte("4"), bbb123)
	}

	ccc123, err := cccTxn.Get([]byte("123"))

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	if bytes.Compare(ccc123, []byte("7")) != 0 {
		t.Fatalf("expected #%v, got %#v", []byte("7"), ccc123)
	}

	aaaIter, err := aaaTxn.Keys(nil, nil, kv.SortOrderAsc)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	bbbIter, err := bbbTxn.Keys(nil, nil, kv.SortOrderAsc)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	cccIter, err := cccTxn.Keys(nil, nil, kv.SortOrderAsc)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	diff := cmp.Diff([][2][]byte{
		{[]byte("123"), []byte("1")},
		{[]byte("456"), []byte("2")},
		{[]byte("789"), []byte("3")},
	}, getSequence(aaaIter))

	if diff != "" {
		t.Fatalf(diff)
	}

	diff = cmp.Diff([][2][]byte{
		{[]byte("123"), []byte("4")},
		{[]byte("456"), []byte("5")},
		{[]byte("789"), []byte("6")},
	}, getSequence(bbbIter))

	if diff != "" {
		t.Fatalf(diff)
	}

	diff = cmp.Diff([][2][]byte{
		{[]byte("123"), []byte("7")},
		{[]byte("456"), []byte("8")},
		{[]byte("789"), []byte("9")},
	}, getSequence(cccIter))

	if diff != "" {
		t.Fatalf(diff)
	}

	aaaTxn.Put([]byte("123"), []byte{})
	aaaTxn.Put([]byte("new"), []byte("stuff"))
	bbbTxn.Delete([]byte("456"))

	iter, err := transaction.Keys(nil, nil, kv.SortOrderAsc)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	diff = cmp.Diff([][2][]byte{
		{[]byte("aaa123"), []byte{}},
		{[]byte("aaa456"), []byte("2")},
		{[]byte("aaa789"), []byte("3")},
		{[]byte("aaanew"), []byte("stuff")},
		{[]byte("bbb123"), []byte("4")},
		{[]byte("bbb789"), []byte("6")},
		{[]byte("ccc123"), []byte("7")},
		{[]byte("ccc456"), []byte("8")},
		{[]byte("ccc789"), []byte("9")},
	}, getSequence(iter))

	if diff != "" {
		t.Fatalf(diff)
	}
}
