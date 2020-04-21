package mvcc_test

import (
	"errors"
	"testing"

	"github.com/jrife/ptarmigan/flock/server/flockpb"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/flock/storage/mvcc"
)

var errAnyError = errors.New("Any error")

type storeChangeset map[string]replicaChangeset

func (changeset storeChangeset) compile() storeState {
	store := storeState{}

	for replicaName, replicaChangeset := range changeset {
		replica := replicaState{metadata: replicaChangeset.metadata, revisions: []revision{}}

		if replica.metadata == nil {
			replica.metadata = []byte{}
		}

		lastRevision := revision{revision: 0, kvs: map[string]flockpb.KeyValue{}}

		for _, changeset := range replicaChangeset.revisions {
			if !changeset.commit {
				continue
			}

			lastRevision = changeset.apply(lastRevision)
			replica.revisions = append(replica.revisions, lastRevision)
		}

		store[replicaName] = replica
	}

	return store
}

type replicaChangeset struct {
	metadata  []byte
	revisions []revisionChangeset
}

type revisionChangeset struct {
	changes map[string][]byte
	commit  bool
}

func (changeset revisionChangeset) apply(rev revision) revision {
	newRevision := revision{revision: rev.revision, kvs: map[string]flockpb.KeyValue{}}

	// Make a copy
	for key, kv := range rev.kvs {
		newRevision.kvs[key] = kv
	}

	// Modify the copy
	for key, value := range changeset.changes {
		if value == nil {
			delete(newRevision.kvs, key)

			continue
		}

		oldKV, ok := newRevision.kvs[key]
		newKV := oldKV

		if !ok {
			newKV.CreateRevision = newRevision.revision
			newKV.Key = []byte(key)
			newKV.Version = 1
		} else {

			newKV.Value = value
			newKV.Version++
		}

		newKV.ModRevision = newRevision.revision
		newKV.Value = value

		newRevision.kvs[key] = newKV
	}

	return newRevision
}

type storeState map[string]replicaState
type replicaState struct {
	metadata  []byte
	revisions []revision
}

type revision struct {
	revision int64
	kvs      map[string]flockpb.KeyValue
}

func newEmptyStore(t *testing.T) mvcc.IStore {
	return nil
}

func newStore(t *testing.T, initialState storeChangeset) mvcc.IStore {
	store := newEmptyStore(t)

	for replicaStoreName, replicaChangeset := range initialState {
		replicaStore := store.ReplicaStore(replicaStoreName)

		if err := replicaStore.Create(replicaChangeset.metadata); err != nil {
			store.Close()
			store.Purge()

			t.Fatalf("failed to create replica store %s: %s", replicaStoreName, err.Error())
		}

		// apply revisions
		for i, revision := range replicaChangeset.revisions {
			rev, err := replicaStore.NewRevision(flockpb.RaftStatus{})

			if err != nil {
				store.Close()
				store.Purge()

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
					store.Close()
					store.Purge()

					t.Fatalf("failed to create revision %d: %s", i, err.Error())
				}
			}

			if revision.commit {
				err = rev.Commit()
			} else {
				err = rev.Abort()
			}

			if err != nil {
				store.Close()
				store.Purge()

				t.Fatalf("failed to create revision %d: %s", i, err.Error())
			}
		}
	}

	return store
}

func getStore(t *testing.T, store mvcc.IStore) storeState {
	next := mvcc.ReplicaStoreMin
	result := storeState{}

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

			if err != nil {
				t.Fatalf("error while getting metadata from replica store %s: %s", replicaStore.Name(), err.Error())
			}

			result[replicaStore.Name()] = replicaState{metadata: metadata, revisions: getAllRevisions(t, replicaStore)}
		}
	}

	return result
}

func getAllRevisions(t *testing.T, replicaStore mvcc.IReplicaStore) []revision {
	result := []revision{}
	view, err := replicaStore.View(mvcc.RevisionOldest)

	if err != nil {
		t.Fatalf("error while trying to read oldest revision: %s", err.Error())
	}

	for {
		result = append(result, revision{kvs: getAllKVs(t, view), revision: view.Revision()})
		view, err = replicaStore.View(view.Revision() + 1)

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

func doMutateTest(t *testing.T, store mvcc.IStore, mutate func(), expectedFinalState storeState) {
	mutate()

	diff := cmp.Diff(expectedFinalState, getStore(t, store))

	if diff != "" {
		t.Fatalf(diff)
	}
}

// TestReplicaStoreCreate tests creation of
// replica stores inside a store.
func TestReplicaStoreCreate(t *testing.T) {
	testCases := map[string]struct {
		initialState storeChangeset
		name         string
		metadata     []byte
	}{
		"add-with-nil-metadata": {
			initialState: storeChangeset{
				"b": {
					revisions: []revisionChangeset{},
					metadata:  []byte{4, 5, 6},
				},
				"c": {
					revisions: []revisionChangeset{},
					metadata:  []byte{7, 8, 9},
				},
			},
			name:     "a",
			metadata: nil,
		},
		"add-with-non-nil-metadata": {
			initialState: storeChangeset{
				"b": {
					revisions: []revisionChangeset{},
					metadata:  []byte{4, 5, 6},
				},
				"c": {
					revisions: []revisionChangeset{},
					metadata:  []byte{7, 8, 9},
				},
			},
			name:     "a",
			metadata: []byte{1, 2, 3},
		},
		"add-existing": {
			initialState: storeChangeset{
				"a": {
					revisions: []revisionChangeset{},
					metadata:  []byte{1, 2, 3},
				},
				"b": {
					revisions: []revisionChangeset{},
					metadata:  []byte{4, 5, 6},
				},
				"c": {
					revisions: []revisionChangeset{},
					metadata:  []byte{7, 8, 9},
				},
			},
			name:     "a",
			metadata: []byte{5, 6, 7},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := newStore(t, testCase.initialState)

			defer store.Purge()
			defer store.Close()

			finalState := testCase.initialState.compile()

			if _, ok := finalState[testCase.name]; !ok {
				metadata := testCase.metadata

				if metadata == nil {
					metadata = []byte{}
				}

				finalState[testCase.name] = replicaState{
					metadata:  metadata,
					revisions: []revision{},
				}
			}

			doMutateTest(t, store, func() {
				if err := store.ReplicaStore(testCase.name).Create(testCase.metadata); err != nil {
					t.Fatalf("error while creating %s: %s", testCase.name, err.Error())
				}
			}, finalState)
		})
	}
}

// TestReplicaStoreDelete tests deletion of
// replica stores inside a store.
func TestReplicaStoreDelete(t *testing.T) {
	testCases := map[string]struct {
		initialState storeChangeset
		name         string
	}{
		"delete-existing": {
			initialState: storeChangeset{
				"a": {
					revisions: []revisionChangeset{},
					metadata:  []byte{1, 2, 3},
				},
				"b": {
					revisions: []revisionChangeset{},
					metadata:  []byte{4, 5, 6},
				},
				"c": {
					revisions: []revisionChangeset{},
					metadata:  []byte{7, 8, 9},
				},
			},
			name: "a",
		},
		"delete-not-existing": {
			initialState: storeChangeset{
				"b": {
					revisions: []revisionChangeset{},
					metadata:  []byte{4, 5, 6},
				},
				"c": {
					revisions: []revisionChangeset{},
					metadata:  []byte{7, 8, 9},
				},
			},
			name: "a",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := newStore(t, testCase.initialState)

			defer store.Purge()
			defer store.Close()

			finalState := testCase.initialState.compile()
			delete(finalState, testCase.name)
			doMutateTest(t, store, func() {
				if err := store.ReplicaStore(testCase.name).Delete(); err != nil {
					t.Fatalf("error while deleting %s: %s", testCase.name, err.Error())
				}
			}, finalState)
		})
	}
}

func TestMVCC(t *testing.T) {
	testCases := map[string]struct {
		initialState storeChangeset
	}{
		"put-existing": {
			initialState: storeChangeset{
				"TEST": {
					revisions: []revisionChangeset{
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
						{
							changes: map[string][]byte{
								"b": []byte("replaced"),
							},
							commit: true,
						},
					},
					metadata: []byte{},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := newStore(t, testCase.initialState)

			defer store.Purge()
			defer store.Close()

			doMutateTest(t, store, func() {}, testCase.initialState.compile())
		})
	}
}

func TestQuery(t *testing.T) {
	testCases := map[string]struct {
		initialState storeChangeset
		query        flockpb.KVQueryRequest
		result       flockpb.KVQueryResponse
	}{
		"put-existing": {
			initialState: storeChangeset{
				"TEST": {
					revisions: []revisionChangeset{
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
						{
							changes: map[string][]byte{
								"b": []byte("replaced"),
							},
							commit: true,
						},
					},
					metadata: []byte{},
				},
			},
			query:  flockpb.KVQueryRequest{},
			result: flockpb.KVQueryResponse{},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := newStore(t, testCase.initialState)

			defer store.Purge()
			defer store.Close()

			doMutateTest(t, store, func() {}, testCase.initialState.compile())
		})
	}
}

func TestLeases(t *testing.T) {
}

func TestRaftStatusEnforcement(t *testing.T) {
}

func TestSnapshots(t *testing.T) {
	// Make two stores, apply snapshot and make sure that the first
	// is identical to the other
}
