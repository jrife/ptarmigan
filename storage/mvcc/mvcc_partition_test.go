package mvcc_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/storage/kv/keys"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

func testPartition(builder tempStoreBuilder, t *testing.T) {
	t.Run("Name", func(t *testing.T) { testPartitionName(builder, t) })
	t.Run("Create", func(t *testing.T) { testPartitionCreate(builder, t) })
	t.Run("Delete", func(t *testing.T) { testPartitionDelete(builder, t) })
	t.Run("Metadata", func(t *testing.T) { testPartitionMetadata(builder, t) })
	t.Run("Begin", func(t *testing.T) { testPartitionBegin(builder, t) })
	t.Run("View", func(t *testing.T) { testPartitionView(builder, t) })
	t.Run("ApplySnapshot", func(t *testing.T) { testPartitionApplySnapshot(builder, t) })
	t.Run("Snapshot", func(t *testing.T) { testPartitionSnapshot(builder, t) })
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

			name := store.Partition(testCase.name).Name()
			diff := cmp.Diff(testCase.name, name)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func testPartitionCreate(builder tempStoreBuilder, t *testing.T) {
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
		metadata     []byte
	}{
		"partition-that-exists": {
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
			name:     []byte("b"),
			metadata: []byte{1, 2, 3},
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
				"c": {
					Metadata:  []byte{1, 2, 3},
					Revisions: []revision{},
				},
			},
			name:     []byte("c"),
			metadata: []byte{1, 2, 3},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			doMutateTest(t, store, func() {
				if err := store.Partition(testCase.name).Create(testCase.metadata); err != nil {
					t.Fatalf("expected error to be nil, got %#v", err)
				}
			}, testCase.finalState)

			store.Close()

			if err := store.Partition(testCase.name).Create(testCase.metadata); err != mvcc.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", mvcc.ErrClosed, err)
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

			doMutateTest(t, store, func() {
				if err := store.Partition(testCase.name).Delete(); err != nil {
					t.Fatalf("expected error to be nil, got %#v", err)
				}
			}, testCase.finalState)

			store.Close()

			if err := store.Partition(testCase.name).Delete(); err != mvcc.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", mvcc.ErrClosed, err)
			}
		})
	}
}

func testPartitionMetadata(builder tempStoreBuilder, t *testing.T) {
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
		name         []byte
		metadata     []byte
		err          error
	}{
		"partition-that-exists": {
			initialState: initialState,
			name:         []byte("b"),
			metadata:     []byte{7, 8, 9},
		},
		"partition-that-doesn't-exist": {
			initialState: initialState,
			name:         []byte("c"),
			metadata:     nil,
			err:          mvcc.ErrNoSuchPartition,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			metadata, err := store.Partition(testCase.name).Metadata()

			if testCase.err == errAnyError {
				if err == nil {
					t.Fatalf("expected any error, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected error to be %#v, got #%v", testCase.err, err)
			}

			if bytes.Compare(metadata, testCase.metadata) != 0 {
				t.Fatalf("expected metadata to be %#v, got %#v", testCase.metadata, metadata)
			}

			store.Close()

			if _, err := store.Partition(testCase.name).Metadata(); err != mvcc.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", mvcc.ErrClosed, err)
			}
		})
	}
}

func testPartitionBegin(builder tempStoreBuilder, t *testing.T) {
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
		name         []byte
		err          error
	}{
		"partition-that-exists": {
			initialState: initialState,
			name:         []byte("b"),
		},
		"partition-that-doesn't-exist": {
			initialState: initialState,
			name:         []byte("c"),
			err:          mvcc.ErrNoSuchPartition,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			transaction, err := store.Partition(testCase.name).Begin()

			if testCase.err == errAnyError {
				if err == nil {
					t.Fatalf("expected any error, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected error to be %#v, got #%v", testCase.err, err)
			}

			if transaction != nil {
				transaction.Rollback()
			}

			store.Close()

			if _, err := store.Partition(testCase.name).Begin(); err != mvcc.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", mvcc.ErrClosed, err)
			}
		})
	}

	t.Run("mutual exclusion", func(t *testing.T) { testPartitionBeginMutualExclusion(builder, t) })
}

func testPartitionBeginMutualExclusion(builder tempStoreBuilder, t *testing.T) {
	store := builder(t, storeChangeset{
		"a": {
			transactions: []transaction{},
			metadata:     []byte{4, 5, 6},
		},
		"b": {
			transactions: []transaction{},
			metadata:     []byte{7, 8, 9},
		},
	})

	var a struct {
		sync.Mutex
		v int
	}

	txn1, err := store.Partition([]byte("a")).Begin()

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	defer txn1.Rollback()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		var txn2 mvcc.Transaction
		txn2, err = store.Partition([]byte("a")).Begin()

		if err != nil {
			return
		}

		defer txn2.Rollback()

		a.Lock()
		a.v++
		a.Unlock()
		wg.Done()
	}()

	// Sleep to make sure goroutine is actually blocked
	time.Sleep(time.Second * 2)

	a.Lock()
	if a.v != 0 {
		t.Fatalf("expected v to be 0, got %#v", a.v)
	}
	a.Unlock()

	txn1.Rollback()
	wg.Wait()

	a.Lock()
	if a.v != 1 {
		t.Fatalf("expected v to be 1, got %#v", a.v)
	}
	a.Unlock()

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}
}

func testPartitionView(builder tempStoreBuilder, t *testing.T) {
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

	// Revisions 1-5
	for i := 0; i < 5; i++ {
		for partitionName, partition := range initialState {
			partition.transactions = append(partition.transactions, transaction{revision: largeRevisionChangeset(), commit: true})
			initialState[partitionName] = partition
		}
	}

	// Compact up to 3 for partition a
	partitionA := initialState["a"]
	partitionA.transactions = append(partitionA.transactions, transaction{compact: 3, revision: revisionChangeset{}, commit: true})
	initialState["a"] = partitionA

	// Revisions 6-10
	for i := 0; i < 5; i++ {
		for partitionName, partition := range initialState {
			partition.transactions = append(partition.transactions, transaction{revision: largeRevisionChangeset(), commit: true})
			initialState[partitionName] = partition
		}
	}

	initialState["c"] = partitionChangeset{
		transactions: []transaction{},
		metadata:     []byte{1, 2, 3},
	}

	testCases := map[string]struct {
		initialState storeChangeset
		name         []byte
		revision     int64
		expected     int64
		err          error
	}{
		"partition-that-doesn't-exist": {
			initialState: initialState,
			name:         []byte("d"),
			revision:     mvcc.RevisionNewest,
			err:          mvcc.ErrNoSuchPartition,
		},
		"newest-revision": {
			initialState: initialState,
			name:         []byte("b"),
			revision:     mvcc.RevisionNewest,
			expected:     10,
		},
		"newest-revision-after-compaction": {
			initialState: initialState,
			name:         []byte("a"),
			revision:     mvcc.RevisionNewest,
			expected:     10,
		},
		"newest-revision-empty-partition": {
			initialState: initialState,
			name:         []byte("c"),
			revision:     mvcc.RevisionNewest,
			err:          mvcc.ErrNoRevisions,
		},
		"oldest-revision": {
			initialState: initialState,
			name:         []byte("b"),
			revision:     mvcc.RevisionOldest,
			expected:     1,
		},
		"oldest-revision-after-compaction": {
			initialState: initialState,
			name:         []byte("a"),
			revision:     mvcc.RevisionOldest,
			expected:     3,
		},
		"oldest-revision-empty-partition": {
			initialState: initialState,
			name:         []byte("c"),
			revision:     mvcc.RevisionOldest,
			err:          mvcc.ErrNoRevisions,
		},
		"highest-revision": {
			initialState: initialState,
			name:         []byte("b"),
			revision:     10,
			expected:     10,
		},
		"lowest-revision": {
			initialState: initialState,
			name:         []byte("b"),
			revision:     1,
			expected:     1,
		},
		"middle-revision": {
			initialState: initialState,
			name:         []byte("b"),
			revision:     5,
			expected:     5,
		},
		"specific-revision-empty-partition": {
			initialState: initialState,
			name:         []byte("c"),
			revision:     5,
			err:          mvcc.ErrNoRevisions,
		},
		"compacted-revision": {
			initialState: initialState,
			name:         []byte("a"),
			revision:     1,
			err:          mvcc.ErrCompacted,
		},
		"future-revision": {
			initialState: initialState,
			name:         []byte("a"),
			revision:     11,
			err:          mvcc.ErrRevisionTooHigh,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("recovered %#v\n", r)
				}
			}()

			store := builder(t, testCase.initialState)

			view, err := store.Partition(testCase.name).View(testCase.revision)

			if view != nil {
				rev := view.Revision()
				view.Close()

				if rev != testCase.expected {
					t.Fatalf("expected revision of view to be %d, got %d", testCase.expected, rev)
				}
			}

			if testCase.err == errAnyError {
				if err == nil {
					t.Fatalf("expected any error, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected error to be %#v, got #%v", testCase.err, err)
			}

			store.Close()

			if _, err := store.Partition(testCase.name).View(testCase.revision); err != mvcc.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", mvcc.ErrClosed, err)
			}
		})
	}
}

func testPartitionApplySnapshot(builder tempStoreBuilder, t *testing.T) {
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
		name         []byte
	}{
		"partition-that-exists": {
			initialState: initialState,
			name:         []byte("b"),
		},
		"partition-that-doesn't-exist": {
			initialState: initialState,
			name:         []byte("c"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			err := store.Partition(testCase.name).ApplySnapshot(bytes.NewReader([]byte{}))

			if err != nil {
				t.Fatalf("expected error to be nil, got #%v", err)
			}

			store.Close()

			if err := store.Partition(testCase.name).ApplySnapshot(bytes.NewReader([]byte{})); err != mvcc.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", mvcc.ErrClosed, err)
			}
		})
	}

	t.Run("e2e", func(t *testing.T) { testPartitionApplySnapshotE2E(builder, t) })
}

func testPartitionApplySnapshotE2E(builder tempStoreBuilder, t *testing.T) {
	changes := largeStoreChangeset()
	sourceStore := builder(t, changes)
	destStore := builder(t, storeChangeset{})

	expectedState := changes.apply(store{})
	partitions, err := sourceStore.Partitions(keys.All(), -1)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	for _, partition := range partitions {
		snap, err := sourceStore.Partition(partition).Snapshot()

		if err != nil {
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		err = destStore.Partition(partition).ApplySnapshot(snap)

		if err != nil {
			t.Fatalf("expected err to be nil, got %#v", err)
		}
	}

	sourceStoreFinalState := getStore(t, sourceStore)
	destStoreFinalState := getStore(t, destStore)

	diff := cmp.Diff(expectedState, sourceStoreFinalState)

	if diff != "" {
		t.Fatalf(diff)
	}

	diff = cmp.Diff(expectedState, destStoreFinalState)

	if diff != "" {
		t.Fatalf(diff)
	}
}

func testPartitionSnapshot(builder tempStoreBuilder, t *testing.T) {
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
		name         []byte
		err          error
	}{
		"partition-that-exists": {
			initialState: initialState,
			name:         []byte("b"),
		},
		"partition-that-doesn't-exist": {
			initialState: initialState,
			name:         []byte("c"),
			err:          mvcc.ErrNoSuchPartition,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			snap, err := store.Partition(testCase.name).Snapshot()

			if testCase.err == errAnyError {
				if err == nil {
					t.Fatalf("expected any error, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expected error to be %#v, got #%v", testCase.err, err)
			}

			if snap != nil {
				snap.Close()
			}

			store.Close()

			if _, err := store.Partition(testCase.name).Snapshot(); err != mvcc.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", mvcc.ErrClosed, err)
			}
		})
	}
}
