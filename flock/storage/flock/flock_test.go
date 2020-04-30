package flock_test

import (
	"bytes"
	"errors"
	"fmt"
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

func (store storeState) query(query flockpb.KVQueryRequest) (flockpb.KVQueryResponse, error) {
	replica, ok := store[query.Header.Replica]

	if !ok {
		return flockpb.KVQueryResponse{}, mvcc.ErrNoSuchReplicaStore
	}

	return replica.query(query)
}

type replicaState struct {
	metadata  []byte
	revisions []revision
}

func (replica replicaState) query(query flockpb.KVQueryRequest) (flockpb.KVQueryResponse, error) {
	revision, err := replica.findRevision(query.Revision)

	if err != nil {
		return flockpb.KVQueryResponse{}, err
	}

	response := revision.query(query)
	// Should be latest revision
	response.Header.Revision = replica.revisions[len(replica.revisions)-1].revision

	return response, nil
}

func (replica *replicaState) compact(rev int64) error {
	switch i := replica.findRevisionI(rev); i {
	case -1:
		return nil
	case len(replica.revisions):
		return mvcc.ErrRevisionTooHigh
	default:
		replica.revisions = replica.revisions[i:]
	}

	return nil
}

func (replica replicaState) findRevision(rev int64) (revision, error) {
	// brand new/no revision
	if len(replica.revisions) == 0 {
		return revision{}, mvcc.ErrRevisionTooHigh
	}

	if rev == 0 {
		// latest revision
		return replica.revisions[len(replica.revisions)-1], nil
	}

	// binary search for revision
	switch i := replica.findRevisionI(rev); i {
	case -1:
		return revision{}, mvcc.ErrCompacted
	case len(replica.revisions):
		return revision{}, mvcc.ErrRevisionTooHigh
	default:
		return replica.revisions[i], nil
	}
}

func (replica replicaState) findRevisionI(rev int64) int {
	low := -1
	high := len(replica.revisions)
	mid := low + (high-low)/2

	for mid >= 0 && mid < len(replica.revisions) && low < high {
		switch r := replica.revisions[mid].revision; {
		case rev < r:
			high = mid - 1
		case rev > r:
			low = mid + 1
		default:
			low = mid
			high = mid
		}

		mid = low + (high-low)/2
	}

	return mid
}

type revision struct {
	revision int64
	kvs      map[string]flockpb.KeyValue
}

func (rev revision) query(query flockpb.KVQueryRequest) flockpb.KVQueryResponse {
	response := flockpb.KVQueryResponse{}
	keyValues := rev.kvList(query.SortTarget, query.SortOrder, query.ExcludeValues)
	prunedKVs := []*flockpb.KeyValue{}

	// Prune kvs that don't match selection
	for _, kv := range keyValues {
		if query.Selection != nil {
			// Key exact match
			if query.Selection.Key != nil && bytes.Compare(kv.Key, query.Selection.Key) != 0 {
				continue
			}

			// Key prefix match
			if query.Selection.KeyStartsWith != nil && !bytes.HasPrefix(kv.Key, query.Selection.KeyStartsWith) {
				continue
			}

			// Lease match
			if query.Selection.Lease != 0 && kv.Lease != query.Selection.Lease {
				continue
			}

			// Key range
			switch query.Selection.KeyRangeMin.(type) {
			case *flockpb.KVSelection_KeyGt:
				if bytes.Compare(kv.Key, query.Selection.GetKeyGt()) <= 0 {
					continue
				}
			case *flockpb.KVSelection_KeyGte:
				if bytes.Compare(kv.Key, query.Selection.GetKeyGte()) < 0 {
					continue
				}
			default:
				continue
			}

			switch query.Selection.KeyRangeMax.(type) {
			case *flockpb.KVSelection_KeyLt:
				if bytes.Compare(kv.Key, query.Selection.GetKeyLt()) >= 0 {
					continue
				}
			case *flockpb.KVSelection_KeyLte:
				if bytes.Compare(kv.Key, query.Selection.GetKeyLte()) > 0 {
					continue
				}
			default:
				continue
			}

			// Mod revision range
			switch query.Selection.ModRevisionStart.(type) {
			case *flockpb.KVSelection_ModRevisionGt:
				if kv.ModRevision <= query.Selection.GetModRevisionGt() {
					continue
				}
			case *flockpb.KVSelection_ModRevisionGte:
				if kv.ModRevision < query.Selection.GetModRevisionGte() {
					continue
				}
			default:
				continue
			}

			switch query.Selection.ModRevisionEnd.(type) {
			case *flockpb.KVSelection_ModRevisionLt:
				if kv.ModRevision >= query.Selection.GetModRevisionLt() {
					continue
				}
			case *flockpb.KVSelection_ModRevisionLte:
				if kv.ModRevision > query.Selection.GetModRevisionLte() {
					continue
				}
			default:
				continue
			}

			// Create revision range
			switch query.Selection.CreateRevisionStart.(type) {
			case *flockpb.KVSelection_CreateRevisionGt:
				if kv.ModRevision <= query.Selection.GetCreateRevisionGt() {
					continue
				}
			case *flockpb.KVSelection_CreateRevisionGte:
				if kv.ModRevision < query.Selection.GetCreateRevisionGte() {
					continue
				}
			default:
				continue
			}

			switch query.Selection.CreateRevisionEnd.(type) {
			case *flockpb.KVSelection_CreateRevisionLt:
				if kv.ModRevision >= query.Selection.GetCreateRevisionLt() {
					continue
				}
			case *flockpb.KVSelection_CreateRevisionLte:
				if kv.ModRevision > query.Selection.GetCreateRevisionLte() {
					continue
				}
			default:
				continue
			}
		}

		prunedKVs = append(prunedKVs, kv)
	}

	if query.IncludeCount {
		response.Count = int64(len(prunedKVs))
	}

	var start int64

	if query.After != "" {
		fmt.Sscanf(query.After, "%d", &start)
		start += 1
	}

	end := start + query.Limit

	if start > int64(len(prunedKVs)) {
		start = int64(len(prunedKVs))
	}

	if end > int64(len(prunedKVs)) {
		end = int64(len(prunedKVs))
	}

	prunedKVs = prunedKVs[start:end]

	response.After = fmt.Sprintf("%d", end-1)
	response.More = end < int64(len(prunedKVs))
	response.Kvs = prunedKVs
	response.Header = &flockpb.ResponseHeader{}

	return response
}

func (rev revision) kvList(sortTarget flockpb.KVQueryRequest_SortTarget, sortOrder flockpb.KVQueryRequest_SortOrder, excludeValues bool) []*flockpb.KeyValue {
	return nil
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
		err          error
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

			view, err := store.ReplicaStore(testCase.query.Header.Replica).View(testCase.query.Revision)

			if err != nil {
				if err != testCase.err {
					t.Fatalf("expected error %#v, got %#v", testCase.err, err)
				}

				return
			}

			response, err := view.Query(testCase.query)

			if err != nil {
				if err != testCase.err {
					t.Fatalf("expected error %#v, got %#v", testCase.err, err)
				}

				return
			}

			diff := cmp.Diff(testCase.result, response)

			if diff != "" {
				t.Fatalf(diff)
			}
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
