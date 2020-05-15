package mvcc_test

import (
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/rand"

	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/ptarmigan/storage/kv"
	"github.com/jrife/ptarmigan/storage/kv/keys"
	"github.com/jrife/ptarmigan/storage/kv/plugins"
	"github.com/jrife/ptarmigan/storage/mvcc"
)

var errAnyError = errors.New("Any error")

func TestMVCC(t *testing.T) {
	for _, plugin := range plugins.Plugins() {
		t.Run(fmt.Sprintf("MVCC(%s)", plugin.Name()), testMVCC(builder(plugin)))
	}
}

func testMVCC(builder tempStoreBuilder) func(t *testing.T) {
	return func(t *testing.T) {
		testDriver(builder, t)
	}
}

func testDriver(builder tempStoreBuilder, t *testing.T) {
	t.Run("Store", func(t *testing.T) { testStore(builder, t) })
	t.Run("Partition", func(t *testing.T) { testPartition(builder, t) })
	t.Run("Transaction", func(t *testing.T) { testTransaction(builder, t) })
	t.Run("Revision", func(t *testing.T) { testRevision(builder, t) })
	t.Run("View", func(t *testing.T) { testView(builder, t) })
}

type storeChangeset map[string]partitionChangeset

func (changeset storeChangeset) apply(s store) store {
	for partitionName, partitionChangeset := range changeset {
		p, ok := s[partitionName]

		if !ok {
			p = partition{Metadata: partitionChangeset.metadata, Revisions: []revision{}}
		}

		s[partitionName] = partitionChangeset.apply(p)
	}

	return s
}

type partitionChangeset struct {
	metadata     []byte
	transactions []transaction
}

func (changeset partitionChangeset) apply(p partition) partition {
	lastRevision := revision{Revision: 0, Kvs: map[string][]byte{}}

	if len(p.Revisions) > 0 {
		lastRevision = p.Revisions[len(p.Revisions)-1]
	}

	for _, transaction := range changeset.transactions {
		if !transaction.commit {
			continue
		}

		if transaction.compact > 0 {
			for i, rev := range p.Revisions {
				if rev.Revision == transaction.compact {
					p.Revisions = p.Revisions[i:]

					break
				}
			}
		}

		lastRevision = transaction.revision.apply(lastRevision)
		p.Revisions = append(p.Revisions, lastRevision)
	}

	return p
}

type transaction struct {
	compact  int64
	revision revisionChangeset
	commit   bool
}

type revisionChangeset struct {
	changes map[string][]byte
}

func (changeset revisionChangeset) apply(rev revision) revision {
	newRevision := revision{Revision: rev.Revision + 1, Kvs: map[string][]byte{}}

	// Make a copy
	for key, value := range rev.Kvs {
		newRevision.Kvs[key] = value
	}

	// Modify the copy
	for key, value := range changeset.changes {
		if value == nil {
			delete(newRevision.Kvs, key)

			continue
		}

		newRevision.Kvs[key] = value
	}

	return newRevision
}

type store map[string]partition
type partition struct {
	Metadata  []byte
	Revisions []revision
}

type revision struct {
	Revision int64
	Kvs      map[string][]byte
}

func newEmptyStore(t *testing.T, kvPlugin kv.Plugin) mvcc.Store {
	rootStore, err := kvPlugin.NewTempRootStore()

	if err != nil {
		t.Fatalf("could not initialize kv root store: %s", err)
	}

	store := rootStore.Store([]byte("temp"))

	if err := store.Create(); err != nil {
		t.Fatalf("could not initialize kv store: %s", err)
	}

	mvccStore, err := mvcc.New(store)

	if err != nil {
		rootStore.Delete()

		t.Fatalf("could not create store: %s", err)
	}

	t.Cleanup(func() {
		mvccStore.Close()
		rootStore.Delete()
	})

	return mvccStore
}

func newStore(t *testing.T, kvPlugin kv.Plugin, initialState storeChangeset) mvcc.Store {
	mvccStore := newEmptyStore(t, kvPlugin)

	applyChanges(t, mvccStore, store{}, initialState)

	return mvccStore
}

func applyChanges(t *testing.T, mvccStore mvcc.Store, currentState store, changes storeChangeset) store {
	for partitionName, partitionChangeset := range changes {
		mvccPartition := mvccStore.Partition([]byte(partitionName))

		if _, ok := currentState[partitionName]; !ok {
			currentState[partitionName] = partition{Metadata: partitionChangeset.metadata, Revisions: []revision{}}
		}

		currentPartition := currentState[partitionName]
		currentRevision := revision{Revision: 0, Kvs: map[string][]byte{}}

		if len(currentPartition.Revisions) > 0 {
			currentRevision = currentPartition.Revisions[len(currentPartition.Revisions)-1]
		}

		if err := mvccPartition.Create(partitionChangeset.metadata); err != nil {
			t.Fatalf("failed to create partition %s: %s", partitionName, err)
		}

		for i, transaction := range partitionChangeset.transactions {
			txn, err := mvccPartition.Begin()

			if err != nil {
				t.Fatalf("failed to create revision %d: %s", i, err)
			}

			if transaction.compact > 0 {
				for i, rev := range currentPartition.Revisions {
					if rev.Revision == transaction.compact {
						currentPartition.Revisions = currentPartition.Revisions[i:]

						break
					}
				}

				if err := txn.Compact(transaction.compact); err != nil {
					t.Fatalf("failed to compact partition %s to revision %d: %s", partitionName, transaction.compact, err)
				}
			}

			if transaction.revision.changes != nil {
				rev, err := txn.NewRevision()

				if err != nil {
					txn.Rollback()

					t.Fatalf("failed to create revision %d: %s", i, err)
				}

				for key, value := range transaction.revision.changes {
					var err error

					if value == nil {
						err = rev.Delete([]byte(key))
					} else {
						err = rev.Put([]byte(key), value)
					}

					if err != nil {
						txn.Rollback()

						t.Fatalf("failed to create revision %d: %s", i, err)
					}
				}

				currentRevision = transaction.revision.apply(currentRevision)
				expectedChanges := getAllChanges(t, rev)
				expectedKVs := getAllKVs(t, rev)

				diff := cmp.Diff(expectedChanges, transaction.revision.changes)

				if diff != "" {
					txn.Rollback()

					t.Fatalf(diff)
				}

				diff = cmp.Diff(expectedKVs, currentRevision.Kvs)

				if diff != "" {
					txn.Rollback()

					t.Fatalf(diff)
				}
			}

			if transaction.commit {
				currentPartition.Revisions = append(currentPartition.Revisions, currentRevision)
				currentState[partitionName] = currentPartition
				err = txn.Commit()
			} else {
				err = txn.Rollback()
			}

			if err != nil {
				t.Fatalf("failed to create revision %d: %s", i, err)
			}
		}
	}

	return currentState
}

func getStore(t *testing.T, mvccStore mvcc.Store) store {
	result := store{}

	partitions, err := mvccStore.Partitions(keys.All(), -1)

	if err != nil {
		t.Fatalf("could not retrieve partitions: %s", err)
	}

	for _, partitionName := range partitions {
		metadata, err := mvccStore.Partition(partitionName).Metadata()

		if err != nil {
			t.Fatalf("error while getting metadata from partition %s: %s", partitionName, err)
		}

		result[string(partitionName)] = partition{Metadata: metadata, Revisions: getAllRevisions(t, mvccStore.Partition(partitionName))}
	}

	return result
}

func getAllRevisions(t *testing.T, partition mvcc.Partition) []revision {
	result := []revision{}
	view, err := partition.View(mvcc.RevisionOldest)

	if err == mvcc.ErrNoRevisions {
		return result
	} else if err != nil {
		t.Fatalf("error while trying to read oldest revision: %s", err)
	}

	for {
		result = append(result, revision{Kvs: getAllKVs(t, view), Revision: view.Revision()})
		view.Close()
		view, err = partition.View(view.Revision() + 1)

		if err == mvcc.ErrRevisionTooHigh {
			break
		}
	}

	return result
}

func getAllKVs(t *testing.T, revision mvcc.View) map[string][]byte {
	result := map[string][]byte{}

	kvs, err := revision.Keys(keys.All(), kv.SortOrderAsc)

	if err != nil {
		t.Fatalf("error while trying to read keys from revision %d: %s", revision.Revision(), err)
	}

	for kvs.Next() {
		result[string(kvs.Key())] = kvs.Value()
	}

	if kvs.Error() != nil {
		t.Fatalf("error while iterating over keys: %s", err)
	}

	return result
}

func getAllChanges(t *testing.T, revision mvcc.View) map[string][]byte {
	result := map[string][]byte{}

	diffs, err := revision.Changes(keys.All(), false)

	if err != nil {
		t.Fatalf("error while trying to read changes from revision %d: %s", revision.Revision(), err)
	}

	for diffs.Next() {
		result[string(diffs.Key())] = diffs.Value()
	}

	if diffs.Error() != nil {
		t.Fatalf("error while iterating over diffs: %s", err)
	}

	return result
}

func doMutateTest(t *testing.T, store mvcc.Store, mutate func(), expectedFinalState store) {
	mutate()

	diff := cmp.Diff(expectedFinalState, getStore(t, store))

	if diff != "" {
		t.Fatalf(diff)
	}
}

type tempStoreBuilder func(t *testing.T, initialState storeChangeset) mvcc.Store

func builder(plugin kv.Plugin) tempStoreBuilder {
	return func(t *testing.T, initialState storeChangeset) mvcc.Store {
		return newStore(t, plugin, initialState)
	}
}

func largeStoreChangeset() storeChangeset {
	result := storeChangeset{}

	for p := 0; p < 10; p++ {
		result[fmt.Sprintf("partition-%d", p)] = largePartitionChangeset()
	}

	return result
}

func largePartitionChangeset() partitionChangeset {
	result := partitionChangeset{
		metadata:     []byte("metadata"),
		transactions: []transaction{},
	}

	rev := 0
	for i := 0; i < 100; i++ {
		txn := transaction{revision: revisionChangeset{changes: map[string][]byte{}}, commit: true}

		switch n := rand.Intn(100); {
		case rev > 0 && n < 5:
			txn.compact = int64(rev)
		default:
			rev++
			txn.revision = largeRevisionChangeset()
		}

		result.transactions = append(result.transactions, txn)
	}

	return result
}

func largeRevisionChangeset() revisionChangeset {
	result := revisionChangeset{
		changes: map[string][]byte{},
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", rand.Intn(20))

		switch n := rand.Intn(100); {
		case n < 50:
			result.changes[key] = nil
		default:
			result.changes[key] = randomBytes()
		}
	}

	return result
}

func randomBytes() []byte {
	result := make([]byte, 20)
	crand.Read(result)

	return result
}
