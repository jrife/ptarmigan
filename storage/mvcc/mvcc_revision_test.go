package mvcc_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func testRevision(builder tempStoreBuilder, t *testing.T) {
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
		op revisionOp
	}{
		"revision-without-any-changes": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{},
			op:           revisionOp{},
		},
		"revision-with-some-changes": {
			initialState: initialState,
			partition:    "a",
			txn:          transaction{},
			op:           revisionOp{}.put([]byte("aaa"), []byte("bbb")).delete([]byte("key-1")),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			mvccStore := builder(t, testCase.initialState)
			initialStoreState := testCase.initialState.apply(store{})
			initialPartitionState := initialStoreState[testCase.partition]
			partitionState := initialPartitionState
			txn, err := mvccStore.Partition([]byte(testCase.partition)).Begin(true)

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

			rev, err := txn.NewRevision()

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			// apply op and make sure the revision's changes, state
			// and revision number look correct
			partitionState = testCase.op.apply(partitionState)
			err = testCase.op.applyToRevision(rev)

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			currentRevision := partitionState.Revisions[len(partitionState.Revisions)-1]

			// Changes should reflect the changes made within this revision
			diff := cmp.Diff(currentRevision.Changes, getAllChanges(t, rev))

			if diff != "" {
				t.Fatalf(diff)
			}

			// The new state of the kv store should be visible to this revision
			diff = cmp.Diff(currentRevision.Kvs, getAllKVs(t, rev))

			if diff != "" {
				t.Fatalf(diff)
			}

			if currentRevision.Revision != rev.Revision() {
				t.Fatalf("expected revision to be %d, got %d", currentRevision.Revision, rev.Revision())
			}

			// The new state of the kv store should not be visible to readers outside this
			// revision before commit
			diff = cmp.Diff(initialStoreState, getStore(t, mvccStore))

			if diff != "" {
				t.Fatalf(diff)
			}

			if err := txn.Commit(); err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			// now changes should be visible to all readers
			initialStoreState[testCase.partition] = partitionState
			diff = cmp.Diff(initialStoreState, getStore(t, mvccStore))

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}
