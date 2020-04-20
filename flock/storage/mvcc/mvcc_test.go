package mvcc_test

import (
	"errors"
	"testing"

	"github.com/jrife/ptarmigan/flock/server/flockpb"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/flock/storage/mvcc"
)

var errAnyError = errors.New("Any error")

func newEmptyStore(t *testing.T) mvcc.IStore {
	return nil
}

func getAllReplicaStoreMetadata(t *testing.T, store mvcc.IStore) map[string][]byte {
	next := mvcc.ReplicaStoreMin
	result := map[string][]byte{}

	for {
		page, err := store.ReplicaStores(next, -1)

		if err != nil {
			t.Fatalf("error while listing all replica stores: %s", err.Error())
		}

		if len(page) == 0 {
			break
		}

		last := page[len(page)-1]
		next = last.Name()

		for _, replicaStore := range page {
			metadata, err := replicaStore.Metadata()

			t.Fatalf("error while retrieving metadata for %s: %s", replicaStore.Name(), err.Error())

			if _, ok := result[replicaStore.Name()]; ok {
				t.Fatalf("interface violation detected: pages should not overlap: saw %s again", replicaStore.Name())
			}

			result[replicaStore.Name()] = metadata
		}
	}

	return result
}

func getAllRevisions(t *testing.T, replicaStore mvcc.IReplicaStore) []map[string]flockpb.KeyValue {
	result := []map[string]flockpb.KeyValue{}
	revision, err := replicaStore.View(mvcc.RevisionOldest)

	if err != nil {
		t.Fatalf("error while trying to read oldest revision: %s", err.Error())
	}

	for {
		result = append(result, getAllKVs(t, revision))
		revision, err = replicaStore.View(revision.Revision() + 1)

		if err == mvcc.ErrRevisionTooHigh {
			break
		}
	}

	return result
}

func getAllKVs(t *testing.T, revision mvcc.IView) map[string]flockpb.KeyValue {
	result := map[string]flockpb.KeyValue{}
	page, err := revision.Query(flockpb.KVQueryRequest{})

	if err != nil {
		t.Fatalf("error while trying to read keys from revision %d: %s", revision.Revision(), err.Error())
	}

	for {
		for _, kv := range page.Kvs {
			result[string(kv.Key)] = *kv
		}

		page, err = revision.Query(flockpb.KVQueryRequest{After: page.After})

		if !page.More {
			break
		}
	}

	return result
}

// TestReplicaStoreCreate tests creation of
// replica stores inside a store.
func TestReplicaStoreCreate(t *testing.T) {
	testCases := map[string]struct {
		initialState map[string][]byte
		finalState   map[string][]byte
		name         string
		metadata     []byte
		err          error
	}{
		"add-with-nil-metadata": {
			initialState: map[string][]byte{
				"b": {3, 4, 5},
				"c": {7, 8, 9},
			},
			name:     "a",
			metadata: nil,
			err:      nil,
			finalState: map[string][]byte{
				"a": {},
				"b": {3, 4, 5},
				"c": {7, 8, 9},
			},
		},
		"add-with-non-nil-metadata": {
			initialState: map[string][]byte{
				"b": {3, 4, 5},
				"c": {7, 8, 9},
			},
			name:     "a",
			metadata: []byte{1, 2, 3},
			err:      nil,
			finalState: map[string][]byte{
				"a": {1, 2, 3},
				"b": {3, 4, 5},
				"c": {7, 8, 9},
			},
		},
		"add-existing": {
			initialState: map[string][]byte{
				"a": {1, 2, 3},
				"b": {3, 4, 5},
				"c": {7, 8, 9},
			},
			name:     "a",
			metadata: []byte{5, 6, 7},
			err:      nil,
			finalState: map[string][]byte{
				"a": {1, 2, 3},
				"b": {3, 4, 5},
				"c": {7, 8, 9},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := newEmptyStore(t)

			defer store.Purge()
			defer store.Close()

			// initialize store
			for replicaName, metadata := range testCase.initialState {
				if err := store.ReplicaStore(replicaName).Create(metadata); err != nil {
					t.Fatalf("failed to create replica store %s: %s", replicaName, err.Error())
				}
			}

			// run test case: create a replica store
			err := store.ReplicaStore(testCase.name).Create(testCase.metadata)

			// make sure the error returned was correct
			switch testCase.err {
			case errAnyError:
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
			default:
				if err != testCase.err {
					t.Fatalf("expected err %#v, got %#v", testCase.err, err)
				}
			}

			// make sure the final state of the store is correct
			finalState := getAllReplicaStoreMetadata(t, store)

			diff := cmp.Diff(testCase.finalState, finalState)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

// TestReplicaStoreDelete tests deletion of
// replica stores inside a store.
func TestReplicaStoreDelete(t *testing.T) {
	testCases := map[string]struct {
		initialState map[string][]byte
		finalState   map[string][]byte
		name         string
		err          error
	}{
		"delete-existing": {
			initialState: map[string][]byte{
				"a": {1, 2, 3},
				"b": {3, 4, 5},
				"c": {7, 8, 9},
			},
			name: "a",
			err:  nil,
			finalState: map[string][]byte{
				"b": {3, 4, 5},
				"c": {7, 8, 9},
			},
		},
		"delete-not-existing": {
			initialState: map[string][]byte{
				"a": {1, 2, 3},
				"c": {7, 8, 9},
			},
			name: "b",
			err:  nil,
			finalState: map[string][]byte{
				"a": {1, 2, 3},
				"c": {7, 8, 9},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := newEmptyStore(t)

			defer store.Purge()
			defer store.Close()

			// initialize store
			for replicaName, metadata := range testCase.initialState {
				if err := store.ReplicaStore(replicaName).Create(metadata); err != nil {
					t.Fatalf("failed to create replica store %s: %s", replicaName, err.Error())
				}
			}

			// run test case: create a replica store
			err := store.ReplicaStore(testCase.name).Delete()

			// make sure the error returned was correct
			switch testCase.err {
			case errAnyError:
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
			default:
				if err != testCase.err {
					t.Fatalf("expected err %#v, got %#v", testCase.err, err)
				}
			}

			// make sure the final state of the store is correct
			finalState := getAllReplicaStoreMetadata(t, store)

			diff := cmp.Diff(testCase.finalState, finalState)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func TestMVCC(t *testing.T) {
	// Starts with some initial state
	// Do something
	// - Create a revision
	// - Compact
	// Compare to expected final state
	type revision struct {
		changes map[string][]byte
		commit  bool
	}

	testCases := map[string]struct {
		revisions  []revision
		finalState []map[string]flockpb.KeyValue
		err        error
	}{
		"put-existing": {
			revisions: []revision{
				{
					changes: map[string][]byte{
						"a": []byte("123"),
						"b": []byte("456"),
					},
					commit: true,
				},
				{
					changes: map[string][]byte{
						"a": []byte("456"),
						"b": []byte("789"),
						"c": []byte("899"),
					},
					commit: true,
				},
			},
			err: nil,
			finalState: []map[string]flockpb.KeyValue{
				{
					"a": flockpb.KeyValue{
						Key:            []byte("a"),
						Value:          []byte("123"),
						CreateRevision: 1,
						ModRevision:    1,
						Version:        1,
					},
					"b": flockpb.KeyValue{
						Key:            []byte("b"),
						Value:          []byte("456"),
						CreateRevision: 1,
						ModRevision:    1,
						Version:        1,
					},
				},
				{
					"a": flockpb.KeyValue{
						Key:            []byte("a"),
						Value:          []byte("456"),
						CreateRevision: 1,
						ModRevision:    2,
						Version:        2,
					},
					"b": flockpb.KeyValue{
						Key:            []byte("b"),
						Value:          []byte("789"),
						CreateRevision: 1,
						ModRevision:    2,
						Version:        2,
					},
					"c": flockpb.KeyValue{
						Key:            []byte("c"),
						Value:          []byte("899"),
						CreateRevision: 1,
						ModRevision:    2,
						Version:        2,
					},
				},
				{
					"a": flockpb.KeyValue{
						Key:            []byte("a"),
						Value:          []byte("456"),
						CreateRevision: 1,
						ModRevision:    2,
						Version:        2,
					},
					"b": flockpb.KeyValue{
						Key:            []byte("b"),
						Value:          []byte("replaced"),
						CreateRevision: 1,
						ModRevision:    3,
						Version:        3,
					},
					"c": flockpb.KeyValue{
						Key:            []byte("c"),
						Value:          []byte("899"),
						CreateRevision: 1,
						ModRevision:    2,
						Version:        2,
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := newEmptyStore(t)

			defer store.Purge()
			defer store.Close()

			replicaStore := store.ReplicaStore("TEST")

			if err := replicaStore.Create([]byte{}); err != nil {
				t.Fatalf("failed to create replica store TEST: %s", err.Error())
			}

			// apply revisions
			for i, revision := range testCase.revisions {
				rev, err := replicaStore.NewRevision(flockpb.RaftStatus{})

				if err != nil {
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
						t.Fatalf("failed to create revision %d: %s", i, err.Error())
					}
				}

				if revision.commit {
					err = rev.Commit()
				} else {
					err = rev.Abort()
				}

				if err != nil {
					t.Fatalf("failed to create revision %d: %s", i, err.Error())
				}
			}

			// make sure the final state of the store is correct
			finalState := getAllRevisions(t, replicaStore)

			diff := cmp.Diff(testCase.finalState, finalState)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func TestQuery(t *testing.T) {

}

func TestLeases(t *testing.T) {
}

func TestRaftStatusEnforcement(t *testing.T) {

}

func TestSnapshots(t *testing.T) {
	// Make two stores, apply snapshot and make sure that the first
	// is identical to the other
}
