package mvcc_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

func testTransaction(builder tempStoreBuilder, t *testing.T) {
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
			partition.transactions = append(partition.transactions, transaction{}.newRevision(largeRevisionOp()).commit())
			initialState[partitionName] = partition
		}
	}

	// Compact up to 3 for partition a
	partitionA := initialState["a"]
	partitionA.transactions = append(partitionA.transactions, transaction{}.compact(3).commit())
	initialState["a"] = partitionA

	// Revisions 6-10
	for i := 0; i < 5; i++ {
		for partitionName, partition := range initialState {
			partition.transactions = append(partition.transactions, transaction{}.newRevision(largeRevisionOp()).commit())
			initialState[partitionName] = partition
		}
	}

	initialState["c"] = partitionChangeset{
		transactions: []transaction{},
		metadata:     []byte{1, 2, 3},
	}

	testCases := map[string]struct {
		// State of store when txn is started
		initialState storeChangeset
		// partition to which to apply txn
		partition string
		// txn is the transaction on top of
		// which op is applied
		txn transaction
		// op is the operation whose result
		// will be compared to err
		op op
		// expected err from op, if any
		err error
	}{
		"only-one-new-revision": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{}.newRevision(revisionOp{}),
			op:           revisionOp{},
			err:          mvcc.ErrTooManyRevisions,
		},
		"compact-empty-partition-1": {
			initialState: initialState,
			partition:    "c",
			txn:          transaction{},
			op:           compactOp(mvcc.RevisionNewest),
			err:          mvcc.ErrNoRevisions,
		},
		"compact-empty-partition-2": {
			initialState: initialState,
			partition:    "c",
			txn:          transaction{},
			op:           compactOp(mvcc.RevisionOldest),
			err:          mvcc.ErrNoRevisions,
		},
		"compact-empty-partition-3": {
			initialState: initialState,
			partition:    "c",
			txn:          transaction{},
			op:           compactOp(3),
			err:          mvcc.ErrNoRevisions,
		},
		"compact-revision-too-high": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{},
			op:           compactOp(11),
			err:          mvcc.ErrRevisionTooHigh,
		},
		"compact-revision-too-low": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{},
			op:           compactOp(2),
			err:          mvcc.ErrCompacted,
		},
		"compact-lowest-revision": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{},
			op:           compactOp(3),
		},
		"compact-highest-revision": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{},
			op:           compactOp(10),
		},
		"compact-oldest-revision": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{},
			op:           compactOp(mvcc.RevisionOldest),
		},
		"compact-newest-revision": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{},
			op:           compactOp(mvcc.RevisionNewest),
		},
		"two-compactions-1": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{}.compact(mvcc.RevisionNewest),
			op:           compactOp(mvcc.RevisionNewest),
		},
		"two-compactions-2": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{}.compact(mvcc.RevisionNewest),
			op:           compactOp(5),
			err:          mvcc.ErrCompacted,
		},
		"compact-before-revision": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{}.compact(8),
			op:           revisionOp{}.put([]byte("something"), []byte("value")),
		},
		"compact-all-before-revision": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{}.compact(mvcc.RevisionNewest),
			op:           revisionOp{}.put([]byte("something"), []byte("value")),
		},
		"compact-after-revision": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{}.newRevision(revisionOp{}.put([]byte("something"), []byte("value"))),
			op:           compactOp(8),
		},
		"compact-all-after-revision": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{}.newRevision(revisionOp{}.put([]byte("something"), []byte("value"))),
			op:           compactOp(mvcc.RevisionNewest),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			mvccStore := builder(t, testCase.initialState)
			initialStoreState := testCase.initialState.apply(store{})
			initialPartitionState := initialStoreState[testCase.partition]
			partitionState := initialPartitionState
			txn, err := mvccStore.Partition([]byte(testCase.partition)).Begin()

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			defer txn.Rollback()

			// apply txn ops
			for _, op := range testCase.txn.ops {
				partitionState = op.apply(partitionState)

				if err := op.applyToTransaction(txn); err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			// apply op and make sure it returns the expected error, if any
			if testCase.err == nil {
				partitionState = testCase.op.apply(partitionState)
			}

			err = testCase.op.applyToTransaction(txn)

			if err != testCase.err {
				t.Fatalf("expected err to be %#v, got %#v", testCase.err, err)
			}

			// changes made within the transaction should not yet be
			// visible to readers
			diff := cmp.Diff(initialStoreState, getStore(t, mvccStore))

			if diff != "" {
				t.Fatalf(diff)
			}

			if err := txn.Commit(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			// now changes should be visible
			initialStoreState[testCase.partition] = partitionState
			diff = cmp.Diff(initialStoreState, getStore(t, mvccStore))

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}
