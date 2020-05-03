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

func (changeset storeChangeset) apply(s store) store {
	for partitionName, partitionChangeset := range changeset {
		p, ok := s[partitionName]

		if !ok {
			p = partition{Metadata: partitionChangeset.metadata, Revisions: []revision{}}
		}

		s[partitionName] = partitionChangeset.apply(p)
	}

	return s
}

type partitionChangeset struct {
	metadata     []byte
	transactions []transaction
}

func (changeset partitionChangeset) apply(p partition) partition {
	lastRevision := revision{Revision: 0, Kvs: map[string][]byte{}}

	if len(p.Revisions) > 0 {
		lastRevision = p.Revisions[len(p.Revisions)-1]
	}

	for _, transaction := range changeset.transactions {
		if !transaction.commit {
			continue
		}

		if transaction.compact > 0 {
			for i, rev := range p.Revisions {
				if rev.Revision == transaction.compact {
					p.Revisions = p.Revisions[i:]

					break
				}
			}
		}

		lastRevision = transaction.revision.apply(lastRevision)
		p.Revisions = append(p.Revisions, lastRevision)
	}

	return p
}

type transaction struct {
	compact  int64
	revision revisionChangeset
	commit   bool
}

type revisionChangeset struct {
	changes map[string][]byte
}

func (changeset revisionChangeset) apply(rev revision) revision {
	newRevision := revision{Revision: rev.Revision + 1, Kvs: map[string][]byte{}}

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
	mvccStore := newEmptyStore(t, kvPlugin)

	applyChanges(t, mvccStore, store{}, initialState)

	return mvccStore
}

func applyChanges(t *testing.T, mvccStore mvcc.Store, currentState store, changes storeChangeset) store {
	for partitionName, partitionChangeset := range changes {
		mvccPartition := mvccStore.Partition([]byte(partitionName))

		if _, ok := currentState[partitionName]; !ok {
			currentState[partitionName] = partition{Metadata: partitionChangeset.metadata, Revisions: []revision{}}
		}

		currentPartition := currentState[partitionName]
		currentRevision := revision{Revision: 0, Kvs: map[string][]byte{}}

		if len(currentPartition.Revisions) > 0 {
			currentRevision = currentPartition.Revisions[len(currentPartition.Revisions)-1]
		}

		if err := mvccPartition.Create(partitionChangeset.metadata); err != nil {
			mvccStore.Close()

			t.Fatalf("failed to create partition %s: %s", partitionName, err.Error())
		}

		for i, transaction := range partitionChangeset.transactions {
			txn, err := mvccPartition.Transaction()

			if err != nil {
				mvccStore.Close()

				t.Fatalf("failed to create revision %d: %s", i, err.Error())
			}

			if transaction.compact > 0 {
				for i, rev := range currentPartition.Revisions {
					if rev.Revision == transaction.compact {
						currentPartition.Revisions = currentPartition.Revisions[i:]

						break
					}
				}

				if err := txn.Compact(transaction.compact); err != nil {
					t.Fatalf("failed to compact partition %s to revision %d: %s", partitionName, transaction.compact, err.Error())
				}
			}

			rev, err := txn.NewRevision()

			if err != nil {
				mvccStore.Close()

				t.Fatalf("failed to create revision %d: %s", i, err.Error())
			}

			for key, value := range transaction.revision.changes {
				var err error

				if value == nil {
					err = rev.Delete([]byte(key))
				} else {
					err = rev.Put([]byte(key), value)
				}

				if err != nil {
					mvccStore.Close()

					t.Fatalf("failed to create revision %d: %s", i, err.Error())
				}
			}

			currentRevision = transaction.revision.apply(currentRevision)
			expectedChanges := getAllChanges(t, rev)
			expectedKVs := getAllKVs(t, rev)

			diff := cmp.Diff(expectedChanges, transaction.revision.changes)

			if diff != "" {
				t.Fatalf(diff)
			}

			diff = cmp.Diff(expectedKVs, currentRevision.Kvs)

			if diff != "" {
				t.Fatalf(diff)
			}

			if transaction.commit {
				currentPartition.Revisions = append(currentPartition.Revisions, currentRevision)
				currentState[partitionName] = currentPartition
				err = txn.Commit()
			} else {
				err = txn.Rollback()
			}

			if err != nil {
				mvccStore.Close()

				t.Fatalf("failed to create revision %d: %s", i, err.Error())
			}
		}
	}

	return currentState
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

	for {
		result = append(result, revision{Kvs: getAllKVs(t, view), Revision: view.Revision()})
		view.Close()
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

func getAllChanges(t *testing.T, revision mvcc.View) map[string][]byte {
	result := map[string][]byte{}

	diffs, err := revision.Changes(nil, nil, -1, false)

	if err != nil {
		t.Fatalf("error while trying to read changes from revision %d: %s", revision.Revision(), err.Error())
	}

	for _, diff := range diffs {
		result[string(diff[0])] = diff[1]
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
	t.Run("Transaction", func(t *testing.T) { testPartitionTransaction(builder, t) })
	t.Run("View", func(t *testing.T) { testPartitionView(builder, t) })
	t.Run("ApplySnapshot", func(t *testing.T) { testPartitionApplySnapshot(builder, t) })
	t.Run("Snapshot", func(t *testing.T) { testPartitionSnapshot(builder, t) })
}

func testPartitionTransaction(builder tempStoreBuilder, t *testing.T) {
	t.Run("NewRevision", func(t *testing.T) { testTransactionNewRevision(builder, t) })
	t.Run("Compact", func(t *testing.T) { testTransactionCompact(builder, t) })
}

func testPartitionView(builder tempStoreBuilder, t *testing.T) {
	t.Run("Keys", func(t *testing.T) { testViewKeys(builder, t) })
	t.Run("Changes", func(t *testing.T) { testViewChanges(builder, t) })
	t.Run("Revision", func(t *testing.T) { testViewRevision(builder, t) })
}

func testStorePartitions(builder tempStoreBuilder, t *testing.T) {
	initialState := storeChangeset{
		"a": {
			transactions: []transaction{},
			metadata:     []byte{4, 5, 6},
		},
		"b": {
			transactions: []transaction{},
			metadata:     []byte{4, 5, 6},
		},
		"c": {
			transactions: []transaction{},
			metadata:     []byte{7, 8, 9},
		},
		"d": {
			transactions: []transaction{},
			metadata:     []byte{7, 8, 9},
		},
		"e": {
			transactions: []transaction{},
			metadata:     []byte{7, 8, 9},
		},
		"f": {
			transactions: []transaction{},
			metadata:     []byte{7, 8, 9},
		},
		"g": {
			transactions: []transaction{},
			metadata:     []byte{7, 8, 9},
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
					transactions: []transaction{},
					metadata:     []byte{4, 5, 6},
				},
			},
			name: []byte("a"),
		},
		"partition-that-does-not-exist": {
			initialState: storeChangeset{
				"a": {
					transactions: []transaction{},
					metadata:     []byte{4, 5, 6},
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
			transactions: []transaction{},
			metadata:     []byte{4, 5, 6},
		},
		"b": {
			transactions: []transaction{},
			metadata:     []byte{7, 8, 9},
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

func testPartitionApplySnapshot(builder tempStoreBuilder, t *testing.T) {
}

func testPartitionSnapshot(builder tempStoreBuilder, t *testing.T) {
}

func testTransactionNewRevision(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		revisions storeChangeset
		revision  revisionChangeset
		err       error
	}{
		"no-compactions": {
			revisions: storeChangeset{
				"a": {
					transactions: []transaction{
						transaction{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key1": []byte("aaa"),
									"key2": []byte("bbb"),
									"key3": []byte("ccc"),
								},
							},
							commit: true,
						},
						transaction{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key3": []byte("ddd"),
									"key4": []byte("eee"),
									"key5": []byte("fff"),
								},
							},
							commit: true,
						},
						transaction{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key2": nil,
									"key4": nil,
								},
							},
							commit: true,
						},
					},
					metadata: []byte{4, 5, 6},
				},
				"b": {
					transactions: []transaction{},
					metadata:     []byte{7, 8, 9},
				},
			},
		},
		"after-compaction": {
			revisions: storeChangeset{
				"a": {
					transactions: []transaction{
						transaction{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key1": []byte("aaa"),
									"key2": []byte("bbb"),
									"key3": []byte("ccc"),
								},
							},
							commit: true,
						},
						transaction{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key3": []byte("ddd"),
									"key4": []byte("eee"),
									"key5": []byte("fff"),
								},
							},
							commit: true,
						},
						transaction{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key2": nil,
									"key4": nil,
								},
							},
							compact: 2,
							commit:  true,
						},
					},
					metadata: []byte{4, 5, 6},
				},
				"b": {
					transactions: []transaction{},
					metadata:     []byte{7, 8, 9},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			mvccStore := builder(t, testCase.revisions)

			defer mvccStore.Close()

			expectedFinalState := testCase.revisions.apply(store{})

			diff := cmp.Diff(expectedFinalState, getStore(t, mvccStore))

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func testTransactionCompact(builder tempStoreBuilder, t *testing.T) {
}

func testViewKeys(builder tempStoreBuilder, t *testing.T) {
}

func testViewChanges(builder tempStoreBuilder, t *testing.T) {
}

func testViewRevision(builder tempStoreBuilder, t *testing.T) {
}
