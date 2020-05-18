package mvcc_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/keys"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

func testView(builder tempStoreBuilder, t *testing.T) {
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

	// Make sure we can retrieve a view for every revision and its state matches what is expected
	mvccStore := builder(t, initialState)
	storeState := initialState.apply(store{})

	for partitionName, partition := range storeState {
		for _, revision := range partition.Revisions {
			func() {
				view, err := mvccStore.Partition([]byte(partitionName)).View(revision.Revision)

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}

				defer view.Close()

				diff := cmp.Diff(revision.Changes, getAllChanges(t, view))

				if diff != "" {
					t.Fatalf(diff)
				}

				diff = cmp.Diff(revision.Kvs, getAllKVs(t, view))

				if diff != "" {
					t.Fatalf(diff)
				}
			}()
		}
	}

	// Some test for these individual functions to make sure they return what is expected
	// with varying key ranges and options.
	t.Run("Keys", func(t *testing.T) { testViewKeys(builder, t) })
	t.Run("Changes", func(t *testing.T) { testViewChanges(builder, t) })
}

func testViewKeys(builder tempStoreBuilder, t *testing.T) {
	initialState := storeChangeset{
		"a": partitionChangeset{
			metadata: []byte{},
			transactions: []transaction{
				transaction{}.newRevision(
					revisionOp{}.
						put([]byte("a"), []byte("1")).
						put([]byte("c"), []byte("2")).
						put([]byte("e"), []byte("3")).
						put([]byte("g"), []byte("4")).
						put([]byte("i"), []byte("5")).
						put([]byte("k"), []byte("6")).
						put([]byte("l"), []byte("7")),
				).commit(),
				transaction{}.newRevision(
					revisionOp{}.
						put([]byte("a"), []byte("b")).
						put([]byte("c"), []byte("d")).
						put([]byte("e"), []byte("f")).
						put([]byte("g"), []byte("h")).
						put([]byte("i"), []byte("j")).
						put([]byte("k"), []byte("l")).
						delete([]byte("l")),
				).commit(),
				transaction{}.newRevision(
					revisionOp{}.
						put([]byte("a"), []byte("7")).
						put([]byte("c"), []byte("8")).
						put([]byte("d"), []byte("asdf")).
						put([]byte("e"), []byte("9")).
						put([]byte("g"), []byte("10")).
						put([]byte("i"), []byte("11")).
						put([]byte("k"), []byte("12")),
				).commit(),
			},
		},
	}

	testCases := map[string]struct {
		initialState storeChangeset
		partition    []byte
		revision     int64
		keys         keys.Range
		order        kv.SortOrder
		kvs          []kv.KV
	}{
		"all-keys-asc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All(),
			order:        kv.SortOrderAsc,
			kvs: []kv.KV{
				{[]byte("a"), []byte("b")},
				{[]byte("c"), []byte("d")},
				{[]byte("e"), []byte("f")},
				{[]byte("g"), []byte("h")},
				{[]byte("i"), []byte("j")},
				{[]byte("k"), []byte("l")},
			},
		},
		"all-keys-desc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All(),
			order:        kv.SortOrderDesc,
			kvs: []kv.KV{
				{[]byte("k"), []byte("l")},
				{[]byte("i"), []byte("j")},
				{[]byte("g"), []byte("h")},
				{[]byte("e"), []byte("f")},
				{[]byte("c"), []byte("d")},
				{[]byte("a"), []byte("b")},
			},
		},
		"bottom-half-keys-asc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Lt([]byte("g")),
			order:        kv.SortOrderAsc,
			kvs: []kv.KV{
				{[]byte("a"), []byte("b")},
				{[]byte("c"), []byte("d")},
				{[]byte("e"), []byte("f")},
			},
		},
		"bottom-half-keys-desc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Lt([]byte("g")),
			order:        kv.SortOrderDesc,
			kvs: []kv.KV{
				{[]byte("e"), []byte("f")},
				{[]byte("c"), []byte("d")},
				{[]byte("a"), []byte("b")},
			},
		},
		"top-half-keys-asc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("g")).Lt([]byte("z")),
			order:        kv.SortOrderAsc,
			kvs: []kv.KV{
				{[]byte("g"), []byte("h")},
				{[]byte("i"), []byte("j")},
				{[]byte("k"), []byte("l")},
			},
		},
		"top-half-keys-desc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("g")).Lt([]byte("z")),
			order:        kv.SortOrderDesc,
			kvs: []kv.KV{
				{[]byte("k"), []byte("l")},
				{[]byte("i"), []byte("j")},
				{[]byte("g"), []byte("h")},
			},
		},
		"middle-keys-asc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("c")).Lt([]byte("i")),
			order:        kv.SortOrderAsc,
			kvs: []kv.KV{
				{[]byte("c"), []byte("d")},
				{[]byte("e"), []byte("f")},
				{[]byte("g"), []byte("h")},
			},
		},
		"middle-keys-desc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("c")).Lt([]byte("i")),
			order:        kv.SortOrderDesc,
			kvs: []kv.KV{
				{[]byte("g"), []byte("h")},
				{[]byte("e"), []byte("f")},
				{[]byte("c"), []byte("d")},
			},
		},
		"sparse-keys-asc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("b")).Lt([]byte("j")),
			order:        kv.SortOrderAsc,
			kvs: []kv.KV{
				{[]byte("c"), []byte("d")},
				{[]byte("e"), []byte("f")},
				{[]byte("g"), []byte("h")},
				{[]byte("i"), []byte("j")},
			},
		},
		"sparse-keys-desc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("b")).Lt([]byte("j")),
			order:        kv.SortOrderDesc,
			kvs: []kv.KV{
				{[]byte("i"), []byte("j")},
				{[]byte("g"), []byte("h")},
				{[]byte("e"), []byte("f")},
				{[]byte("c"), []byte("d")},
			},
		},
		"min=max asc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("e")).Lt([]byte("e")),
			order:        kv.SortOrderAsc,
			kvs:          []kv.KV{},
		},
		"min>max asc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("g")).Lt([]byte("c")),
			order:        kv.SortOrderDesc,
			kvs:          []kv.KV{},
		},
		"min=max desc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("e")).Lt([]byte("e")),
			order:        kv.SortOrderDesc,
			kvs:          []kv.KV{},
		},
		"min>max desc": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All().Gte([]byte("g")).Lt([]byte("c")),
			order:        kv.SortOrderDesc,
			kvs:          []kv.KV{},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			view, err := store.Partition(testCase.partition).View(testCase.revision)

			if err != nil {
				t.Fatalf("expected error to be nil, got #%v", err)
			}

			defer view.Close()

			kvIter, err := view.Keys(testCase.keys, testCase.order)

			if err != nil {
				t.Fatalf("expected error to be nil, got #%v", err)
			}

			kvs, err := kv.Keys(kvIter, -1)

			if err != nil {
				t.Fatalf("expected error to be nil, got #%v", err)
			}

			diff := cmp.Diff(testCase.kvs, kvs)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func testViewChanges(builder tempStoreBuilder, t *testing.T) {
	initialState := storeChangeset{
		"a": partitionChangeset{
			metadata: []byte{},
			transactions: []transaction{
				transaction{}.newRevision(
					revisionOp{}.
						put([]byte("a"), []byte("1")).
						put([]byte("c"), []byte("2")).
						put([]byte("e"), []byte("3")).
						put([]byte("g"), []byte("4")).
						put([]byte("k"), []byte("6")),
				).commit(),
				transaction{}.newRevision(
					revisionOp{}.
						put([]byte("a"), []byte("b")).
						put([]byte("c"), []byte("d")).
						put([]byte("e"), []byte("f")).
						put([]byte("g"), []byte("h")).
						put([]byte("i"), []byte("j")).
						delete([]byte("k")).
						delete([]byte("z")),
				).commit(),
				transaction{}.newRevision(
					revisionOp{}.
						put([]byte("a"), []byte("7")).
						put([]byte("c"), []byte("8")).
						put([]byte("d"), []byte("asdf")).
						put([]byte("e"), []byte("9")).
						put([]byte("g"), []byte("10")).
						put([]byte("i"), []byte("11")).
						put([]byte("k"), []byte("12")),
				).commit(),
			},
		},
	}

	testCases := map[string]struct {
		initialState storeChangeset
		partition    []byte
		revision     int64
		keys         keys.Range
		includePrev  bool
		diffs        []mvcc.Diff
	}{
		"all-keys-asc-no-prev": {
			initialState: initialState,
			partition:    []byte("a"),
			revision:     2,
			keys:         keys.All(),
			includePrev:  false,
			diffs: []mvcc.Diff{
				{[]byte("a"), []byte("b"), nil},
				{[]byte("c"), []byte("d"), nil},
				{[]byte("e"), []byte("f"), nil},
				{[]byte("g"), []byte("h"), nil},
				{[]byte("i"), []byte("j"), nil},
				{[]byte("k"), nil, nil},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := builder(t, testCase.initialState)

			view, err := store.Partition(testCase.partition).View(testCase.revision)

			if err != nil {
				t.Fatalf("expected error to be nil, got #%v", err)
			}

			defer view.Close()

			diffsIter, err := view.Changes(testCase.keys, testCase.includePrev)

			if err != nil {
				t.Fatalf("expected error to be nil, got #%v", err)
			}

			diffs, err := mvcc.Diffs(diffsIter, -1)

			if err != nil {
				t.Fatalf("expected error to be nil, got #%v", err)
			}

			diff := cmp.Diff(testCase.diffs, diffs)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}
