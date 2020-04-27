package mvcc_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/plugins"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

var errAnyError = errors.New("Any error")

type storeChangeset map[string]partitionChangeset

func (changeset storeChangeset) compile() store {
	s := store{}

	for partitionName, partitionChangeset := range changeset {
		p := partition{Metadata: partitionChangeset.metadata, Revisions: []revision{}}
		lastRevision := revision{Revision: 0, Kvs: map[string][]byte{}}

		for _, changeset := range partitionChangeset.revisions {
			if !changeset.commit {
				continue
			}

			lastRevision = changeset.apply(lastRevision)
			p.Revisions = append(p.Revisions, lastRevision)
		}

		s[partitionName] = p
	}

	return s
}

type partitionChangeset struct {
	metadata  []byte
	revisions []revisionChangeset
}

type revisionChangeset struct {
	changes map[string][]byte
	commit  bool
}

func (changeset revisionChangeset) apply(rev revision) revision {
	newRevision := revision{Revision: rev.Revision, Kvs: map[string][]byte{}}

	// Make a copy
	for key, value := range rev.Kvs {
		newRevision.Kvs[key] = value
	}

	// Modify the copy
	for key, value := range changeset.changes {
		if value == nil {
			delete(newRevision.Kvs, key)

			continue
		}

		newRevision.Kvs[key] = value
	}

	return newRevision
}

type store map[string]partition
type partition struct {
	Metadata  []byte
	Revisions []revision
}

type revision struct {
	Revision int64
	Kvs      map[string][]byte
}

var _ mvcc.Store = (*mvccStoreWithCloser)(nil)

type mvccStoreWithCloser struct {
	mvcc.Store
	close func()
}

func (store *mvccStoreWithCloser) Close() error {
	store.close()

	return store.Store.Close()
}

func newEmptyStore(t *testing.T, kvPlugin kv.Plugin) mvcc.Store {
	rootStore, err := kvPlugin.NewTempRootStore()

	if err != nil {
		t.Fatalf("could not initialize kv root store: %s", err.Error())
	}

	store := rootStore.Store([]byte("temp"))

	if err := store.Create(); err != nil {
		t.Fatalf("could not initialize kv store: %s", err.Error())
	}

	mvccStore := &mvccStoreWithCloser{Store: mvcc.New(store), close: func() { rootStore.Delete() }}

	if err := mvccStore.Open(); err != nil {
		rootStore.Delete()

		return nil
	}

	return mvccStore
}

func newStore(t *testing.T, kvPlugin kv.Plugin, initialState storeChangeset) mvcc.Store {
	store := newEmptyStore(t, kvPlugin)

	for partitionName, partitionChangeset := range initialState {
		partition := store.Partition([]byte(partitionName))

		if err := partition.Create(partitionChangeset.metadata); err != nil {
			store.Close()

			t.Fatalf("failed to create partition %s: %s", partitionName, err.Error())
		}

		// apply revisions
		for i, revision := range partitionChangeset.revisions {
			txn, err := partition.Transaction()

			if err != nil {
				store.Close()

				t.Fatalf("failed to create revision %d: %s", i, err.Error())
			}

			rev, err := txn.NewRevision()

			if err != nil {
				store.Close()

				t.Fatalf("failed to create revision %d: %s", i, err.Error())
			}

			for key, value := range revision.changes {
				var err error

				if value == nil {
					err = rev.Delete([]byte(key))
				} else {
					err = rev.Put([]byte(key), value)
				}

				if err != nil {
					store.Close()

					t.Fatalf("failed to create revision %d: %s", i, err.Error())
				}
			}

			if revision.commit {
				err = txn.Commit()
			} else {
				err = txn.Rollback()
			}

			if err != nil {
				store.Close()

				t.Fatalf("failed to create revision %d: %s", i, err.Error())
			}
		}
	}

	return store
}

func getStore(t *testing.T, mvccStore mvcc.Store) store {
	result := store{}

	partitions, err := mvccStore.Partitions(nil, nil, -1)

	if err != nil {
		t.Fatalf("could not retrieve partitions: %s", err.Error())
	}

	for _, partitionName := range partitions {
		metadata, err := mvccStore.Partition(partitionName).Metadata()

		if err != nil {
			t.Fatalf("error while getting metadata from partition %s: %s", partitionName, err.Error())
		}

		fmt.Printf("metadata = %#v\n", metadata)

		result[string(partitionName)] = partition{Metadata: metadata, Revisions: getAllRevisions(t, mvccStore.Partition(partitionName))}
	}

	return result
}

func getAllRevisions(t *testing.T, partition mvcc.Partition) []revision {
	result := []revision{}
	view, err := partition.View(mvcc.RevisionOldest)

	if err == mvcc.ErrNoRevisions {
		return result
	} else if err != nil {
		t.Fatalf("error while trying to read oldest revision: %s", err.Error())
	}

	defer view.Close()

	for {
		result = append(result, revision{Kvs: getAllKVs(t, view), Revision: view.Revision()})
		view, err = partition.View(view.Revision() + 1)

		if err == mvcc.ErrRevisionTooHigh {
			break
		}
	}

	return result
}

func getAllKVs(t *testing.T, revision mvcc.View) map[string][]byte {
	result := map[string][]byte{}

	kvs, err := revision.Keys(nil, nil, -1, mvcc.SortOrderAsc)

	if err != nil {
		t.Fatalf("error while trying to read keys from revision %d: %s", revision.Revision(), err.Error())
	}

	for _, kv := range kvs {
		result[string(kv[0])] = kv[1]
	}

	return result
}

func doMutateTest(t *testing.T, store mvcc.Store, mutate func(), expectedFinalState store) {
	mutate()

	diff := cmp.Diff(expectedFinalState, getStore(t, store))

	if diff != "" {
		t.Fatalf(diff)
	}
}

type tempStoreBuilder func(t *testing.T, initialState storeChangeset) mvcc.Store

func builder(plugin kv.Plugin) tempStoreBuilder {
	return func(t *testing.T, initialState storeChangeset) mvcc.Store {
		return newStore(t, plugin, initialState)
	}
}

func TestMVCC(t *testing.T) {
	pluginManager := plugins.NewKVPluginManager()

	for _, plugin := range pluginManager.Plugins() {
		t.Run(fmt.Sprintf("MVCC(%s)", plugin.Name()), testMVCC(builder(plugin)))
	}
}

func testMVCC(builder tempStoreBuilder) func(t *testing.T) {
	return func(t *testing.T) {
		testDriver(builder, t)
	}
}

func testDriver(builder tempStoreBuilder, t *testing.T) {
	t.Run("Store", func(t *testing.T) { testStore(builder, t) })
	t.Run("Partition", func(t *testing.T) { testPartition(builder, t) })
}

func testStore(builder tempStoreBuilder, t *testing.T) {
	t.Run("Partitions", func(t *testing.T) { testStorePartitions(builder, t) })
}

func testPartition(builder tempStoreBuilder, t *testing.T) {
	t.Run("Name", func(t *testing.T) { testPartitionName(builder, t) })
	t.Run("Delete", func(t *testing.T) { testPartitionDelete(builder, t) })
	t.Run("Metadata", func(t *testing.T) { testPartitionMetadata(builder, t) })
	t.Run("Transaction", func(t *testing.T) { testPartitionTransaction(builder, t) })
	t.Run("View", func(t *testing.T) { testPartitionView(builder, t) })
	t.Run("ApplySnapshot", func(t *testing.T) { testPartitionApplySnapshot(builder, t) })
	t.Run("Snapshot", func(t *testing.T) { testPartitionSnapshot(builder, t) })
}

func testTransaction(builder tempStoreBuilder, t *testing.T) {
	t.Run("NewRevision", func(t *testing.T) { testTransactionNewRevision(builder, t) })
	t.Run("Compact", func(t *testing.T) { testTransactionCompact(builder, t) })
}

func testView(builder tempStoreBuilder, t *testing.T) {
	t.Run("Keys", func(t *testing.T) { testViewKeys(builder, t) })
	t.Run("Changes", func(t *testing.T) { testViewChanges(builder, t) })
	t.Run("Revision", func(t *testing.T) { testViewRevision(builder, t) })
}

func testStorePartitions(builder tempStoreBuilder, t *testing.T) {
	initialState := storeChangeset{
		"a": {
			revisions: []revisionChangeset{},
			metadata:  []byte{4, 5, 6},
		},
		"b": {
			revisions: []revisionChangeset{},
			metadata:  []byte{4, 5, 6},
		},
		"c": {
			revisions: []revisionChangeset{},
			metadata:  []byte{7, 8, 9},
		},
		"d": {
			revisions: []revisionChangeset{},
			metadata:  []byte{7, 8, 9},
		},
		"e": {
			revisions: []revisionChangeset{},
			metadata:  []byte{7, 8, 9},
		},
		"f": {
			revisions: []revisionChangeset{},
			metadata:  []byte{7, 8, 9},
		},
		"g": {
			revisions: []revisionChangeset{},
			metadata:  []byte{7, 8, 9},
		},
	}
	testCases := map[string]struct {
		initialState storeChangeset
		min          []byte
		max          []byte
		limit        int
		result       [][]byte
	}{
		"list-all-1": {
			initialState: initialState,
			min:          nil,
			max:          nil,
			limit:        -1,
			result: [][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
				[]byte("d"),
				[]byte("e"),
				[]byte("f"),
				[]byte("g"),
			},
		},
		"list-all-2": {
			initialState: initialState,
			min:          nil,
			max:          nil,
			limit:        7,
			result: [][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
				[]byte("d"),
				[]byte("e"),
				[]byte("f"),
				[]byte("g"),
			},
		},
		"list-all-3": {
			initialState: initialState,
			min:          []byte("a"),
			max:          []byte("h"),
			limit:        7,
			result: [][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
				[]byte("d"),
				[]byte("e"),
				[]byte("f"),
				[]byte("g"),
			},
		},
		"list-first-half-1": {
			initialState: initialState,
			min:          nil,
			max:          nil,
			limit:        3,
			result: [][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
			},
		},
		"list-first-half-2": {
			initialState: initialState,
			min:          nil,
			max:          []byte("d"),
			limit:        -1,
			result: [][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
			},
		},
		"list-first-half-3": {
			initialState: initialState,
			min:          []byte("a"),
			max:          []byte("d"),
			limit:        -1,
			result: [][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
			},
		},
		"list-last-half-1": {
			initialState: initialState,
			min:          []byte("d"),
			max:          nil,
			limit:        -1,
			result: [][]byte{
				[]byte("d"),
				[]byte("e"),
				[]byte("f"),
				[]byte("g"),
			},
		},
		"list-last-half-2": {
			initialState: initialState,
			min:          []byte("d"),
			max:          nil,
			limit:        4,
			result: [][]byte{
				[]byte("d"),
				[]byte("e"),
				[]byte("f"),
				[]byte("g"),
			},
		},
		"list-last-half-3": {
			initialState: initialState,
			min:          []byte("d"),
			max:          []byte("h"),
			limit:        -1,
			result: [][]byte{
				[]byte("d"),
				[]byte("e"),
				[]byte("f"),
				[]byte("g"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			defer store.Close()

			partitions, err := store.Partitions(testCase.min, testCase.max, testCase.limit)

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			diff := cmp.Diff(testCase.result, partitions)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func testPartitionName(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		initialState storeChangeset
		name         []byte
	}{
		"partition-that-exists": {
			initialState: storeChangeset{
				"a": {
					revisions: []revisionChangeset{},
					metadata:  []byte{4, 5, 6},
				},
			},
			name: []byte("a"),
		},
		"partition-that-does-not-exist": {
			initialState: storeChangeset{
				"a": {
					revisions: []revisionChangeset{},
					metadata:  []byte{4, 5, 6},
				},
			},
			name: []byte("b"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			defer store.Close()

			name := store.Partition(testCase.name).Name()
			diff := cmp.Diff(testCase.name, name)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func testPartitionDelete(builder tempStoreBuilder, t *testing.T) {
	initialState := storeChangeset{
		"a": {
			revisions: []revisionChangeset{},
			metadata:  []byte{4, 5, 6},
		},
		"b": {
			revisions: []revisionChangeset{},
			metadata:  []byte{7, 8, 9},
		},
	}
	testCases := map[string]struct {
		initialState storeChangeset
		finalState   store
		name         []byte
	}{
		"partition-that-exists": {
			initialState: initialState,
			finalState: store{
				"b": {
					Metadata:  []byte{7, 8, 9},
					Revisions: []revision{},
				},
			},
			name: []byte("a"),
		},
		"partition-that-doesn't-exist": {
			initialState: initialState,
			finalState: store{
				"a": {
					Metadata:  []byte{4, 5, 6},
					Revisions: []revision{},
				},
				"b": {
					Metadata:  []byte{7, 8, 9},
					Revisions: []revision{},
				},
			},
			name: []byte("c"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			defer store.Close()

			doMutateTest(t, store, func() {
				if err := store.Partition(testCase.name).Delete(); err != nil {
					t.Fatalf("expected error to be nil, got %#v", err)
				}
			}, testCase.finalState)
		})
	}
}

func testPartitionMetadata(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionTransaction(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionView(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionApplySnapshot(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionSnapshot(builder tempStoreBuilder, t *testing.T) {
}

func testTransactionNewRevision(builder tempStoreBuilder, t *testing.T) {
}

func testTransactionCompact(builder tempStoreBuilder, t *testing.T) {
}

func testViewKeys(builder tempStoreBuilder, t *testing.T) {
}

func testViewChanges(builder tempStoreBuilder, t *testing.T) {
}

func testViewRevision(builder tempStoreBuilder, t *testing.T) {
}
