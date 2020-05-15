package mvcc_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func testTransaction(builder tempStoreBuilder, t *testing.T) {
	t.Run("NewRevision", func(t *testing.T) { testTransactionNewRevision(builder, t) })
	t.Run("Compact", func(t *testing.T) { testTransactionCompact(builder, t) })
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
						{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key1": []byte("aaa"),
									"key2": []byte("bbb"),
									"key3": []byte("ccc"),
								},
							},
							commit: true,
						},
						{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key3": []byte("ddd"),
									"key4": []byte("eee"),
									"key5": []byte("fff"),
								},
							},
							commit: true,
						},
						{
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
						{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key1": []byte("aaa"),
									"key2": []byte("bbb"),
									"key3": []byte("ccc"),
								},
							},
							commit: true,
						},
						{
							revision: revisionChangeset{
								changes: map[string][]byte{
									"key3": []byte("ddd"),
									"key4": []byte("eee"),
									"key5": []byte("fff"),
								},
							},
							commit: true,
						},
						{
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
