package mvcc_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/storage/kv/keys"
	"github.com/jrife/flock/storage/mvcc"
)

func testStore(builder tempStoreBuilder, t *testing.T) {
	t.Run("Partitions", func(t *testing.T) { testStorePartitions(builder, t) })
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
			min:          keys.All().Min,
			max:          keys.All().Max,
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
			min:          keys.All().Min,
			max:          keys.All().Max,
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
			min:          keys.All().Gte([]byte("a")).Min,
			max:          keys.All().Lt([]byte("h")).Max,
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

			partitions, err := store.Partitions(keys.Range{Min: testCase.min, Max: testCase.max}, testCase.limit)

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			diff := cmp.Diff(testCase.result, partitions)

			if diff != "" {
				t.Fatalf(diff)
			}

			store.Close()

			_, err = store.Partitions(keys.Range{Min: testCase.min, Max: testCase.max}, testCase.limit)

			if err != mvcc.ErrClosed {
				t.Fatalf("expected err to be #%v, got %#v", mvcc.ErrClosed, err)
			}
		})
	}
}
