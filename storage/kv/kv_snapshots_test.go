package kv_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jrife/ptarmigan/storage/kv"
)

type snapshotTestCase struct {
	sourceInitialState storeModel
	destInitialState   storeModel
	snapshotStore      []string
}

func (tc snapshotTestCase) run(builder tempStoreBuilder, t *testing.T) {
	sourceStore := builder(t, tc.sourceInitialState)
	destStore := builder(t, tc.destInitialState)
	defer sourceStore.Delete()
	defer destStore.Delete()

	var snapshotSource kv.SubStore = sourceStore
	var snapshotDest kv.SubStore = destStore

	expectedDestFinalState := tc.calculateExpectedFinalState(t)

	for _, subStore := range tc.snapshotStore {
		snapshotSource = snapshotSource.Namespace([]byte(subStore))
		snapshotDest = snapshotDest.Namespace([]byte(subStore))
	}

	snapshot, err := snapshotSource.Snapshot()

	if err != nil {
		t.Fatalf("Could not take snapshot: %s", err.Error())
	}

	if err := snapshotDest.ApplySnapshot(snapshot); err != nil {
		t.Fatalf("Could not apply snapshot: %s", err.Error())
	}

	destFinalState, err := readStore(destStore)

	if err != nil {
		t.Fatalf("Could not read destination store: %s", err.Error())
	}

	if !reflect.DeepEqual(destFinalState, expectedDestFinalState) {
		t.Fatalf("%+v != %+v", destFinalState, expectedDestFinalState)
	}
}

func (tc snapshotTestCase) calculateExpectedFinalState(t *testing.T) storeModel {
	if len(tc.snapshotStore) == 0 {
		tc.destInitialState = tc.sourceInitialState

		return tc.destInitialState
	}

	source := (bucketModel)(tc.sourceInitialState)
	dest := (bucketModel)(tc.destInitialState)

	for i := 0; i < len(tc.snapshotStore)-1; i++ {
		if _, ok := source[tc.snapshotStore[i]]; !ok {
			t.Fatalf("Snapshot target does not exist in source store: %v", tc.snapshotStore)
		}

		if _, ok := source[tc.snapshotStore[i]].(bucketModel); !ok {
			t.Fatalf("Snapshot target is not a substore in the source store: %v", tc.snapshotStore)
		}

		if _, ok := dest[tc.snapshotStore[i]]; !ok {
			t.Fatalf("Snapshot target does not exist in destination store: %v", tc.snapshotStore)
		}

		if _, ok := dest[tc.snapshotStore[i]].(bucketModel); !ok {
			t.Fatalf("Snapshot target is not a substore in the destionation store: %v", tc.snapshotStore)
		}

		source = source[tc.snapshotStore[i]].(bucketModel)
		dest = dest[tc.snapshotStore[i]].(bucketModel)
	}

	dest[tc.snapshotStore[len(tc.snapshotStore)-1]] = source[tc.snapshotStore[len(tc.snapshotStore)-1]]

	return tc.destInitialState
}

func testSnapshots(builder tempStoreBuilder, t *testing.T) {
	testCases := []snapshotTestCase{
		snapshotTestCase{
			sourceInitialState: storeModel{
				"a": bucketModel{
					"x": []byte("value1"),
				},
				"b": bucketModel{
					"y": []byte("value2"),
					"inner": bucketModel{
						"ok": []byte("xyz"),
					},
					"innerEmpty": bucketModel{},
				},
				"c": bucketModel{
					"z": []byte("value3"),
				},
			},
			destInitialState: storeModel{},
			snapshotStore:    []string{},
		},
		snapshotTestCase{
			sourceInitialState: storeModel{
				"a": bucketModel{
					"x": []byte("value1"),
				},
				"b": bucketModel{
					"y": []byte("value2"),
					"inner": bucketModel{
						"ok": []byte("xyz"),
					},
					"innerEmpty": bucketModel{},
				},
				"c": bucketModel{
					"z": []byte("value3"),
				},
			},
			destInitialState: storeModel{
				"b": bucketModel{},
			},
			snapshotStore: []string{"b"},
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) { testCase.run(builder, t) })
	}
}
