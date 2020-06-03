package mvcc_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/storage/mvcc"
)

func testPartition(builder tempStoreBuilder, t *testing.T) {
	t.Run("Name", func(t *testing.T) { testPartitionName(builder, t) })
	t.Run("Create", func(t *testing.T) { testPartitionCreate(builder, t) })
	t.Run("Delete", func(t *testing.T) { testPartitionDelete(builder, t) })
	t.Run("Metadata", func(t *testing.T) { testPartitionMetadata(builder, t) })
	t.Run("Begin", func(t *testing.T) { testPartitionBegin(builder, t) })
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

			transaction, err := store.Partition(testCase.name).Begin(true)

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

			if _, err := store.Partition(testCase.name).Begin(true); err != mvcc.ErrClosed {
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

	txn1, err := store.Partition([]byte("a")).Begin(true)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	defer txn1.Rollback()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		var txn2 mvcc.Transaction
		txn2, err = store.Partition([]byte("a")).Begin(true)

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

			err := store.Partition(testCase.name).ApplySnapshot(context.Background(), bytes.NewReader([]byte{}))

			if err != nil {
				t.Fatalf("expected error to be nil, got #%v", err)
			}

			store.Close()

			if err := store.Partition(testCase.name).ApplySnapshot(context.Background(), bytes.NewReader([]byte{})); err != mvcc.ErrClosed {
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
		snap, err := sourceStore.Partition(partition).Snapshot(context.Background())

		if err != nil {
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		err = destStore.Partition(partition).ApplySnapshot(context.Background(), snap)

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

			snap, err := store.Partition(testCase.name).Snapshot(context.Background())

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

			if _, err := store.Partition(testCase.name).Snapshot(context.Background()); err != mvcc.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", mvcc.ErrClosed, err)
			}
		})
	}
}
