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
	for _, transaction := range changeset.transactions {
		if !transaction.shouldCommit {
			continue
		}

		p = transaction.apply(p)
	}

	return p
}

type transaction struct {
	ops          []op
	shouldCommit bool
}

func (txn transaction) apply(p partition) partition {
	if !txn.shouldCommit {
		return p
	}

	for _, op := range txn.ops {
		p = op.apply(p)
	}

	return p
}

func (txn transaction) compact(r int64) transaction {
	txn.ops = append(txn.ops, compactOp(r))

	return txn
}

func (txn transaction) newRevision(revision revisionOp) transaction {
	txn.ops = append(txn.ops, revision)

	return txn
}

func (txn transaction) commit() transaction {
	txn.shouldCommit = true

	return txn
}

func (txn transaction) rollback() transaction {
	txn.shouldCommit = false

	return txn
}

type op interface {
	apply(partition) partition
	applyToTransaction(mvcc.Transaction) error
}

type compactOp int64

func (revision compactOp) apply(p partition) partition {
	if revision == compactOp(mvcc.RevisionNewest) && len(p.Revisions) != 0 {
		revision = compactOp(p.Revisions[len(p.Revisions)-1].Revision)
	} else if revision == compactOp(mvcc.RevisionOldest) && len(p.Revisions) != 0 {
		revision = compactOp(p.Revisions[0].Revision)
	}

	if revision <= 0 {
		return p
	}

	for i, rev := range p.Revisions {
		if rev.Revision == int64(revision) {
			p.Revisions = p.Revisions[i:]

			break
		}
	}

	return p
}

func (revision compactOp) applyToTransaction(t mvcc.Transaction) error {
	return t.Compact(int64(revision))
}

type revisionOp map[string][]byte

func (r revisionOp) put(k, v []byte) revisionOp {
	r[string(k)] = v

	return r
}

func (r revisionOp) delete(k []byte) revisionOp {
	r[string(k)] = nil

	return r
}

func (r revisionOp) apply(p partition) partition {
	rev := revision{}

	if len(p.Revisions) > 0 {
		rev = p.Revisions[len(p.Revisions)-1]
	}

	newRevision := revision{Revision: rev.Revision + 1, Kvs: map[string][]byte{}, Changes: map[string][]byte{}}

	// Make a copy
	for key, value := range rev.Kvs {
		newRevision.Kvs[key] = value
	}

	// Modify the copy
	for key, value := range r {
		if value == nil {
			if _, ok := newRevision.Kvs[key]; ok {
				delete(newRevision.Kvs, key)
				newRevision.Changes[key] = nil
			}

			continue
		}

		newRevision.Kvs[key] = value
		newRevision.Changes[key] = value
	}

	p.Revisions = append(p.Revisions, newRevision)

	return p
}

func (r revisionOp) applyToTransaction(t mvcc.Transaction) error {
	rev, err := t.NewRevision()

	if err != nil {
		return err
	}

	return r.applyToRevision(rev)
}

func (r revisionOp) applyToRevision(rev mvcc.Revision) error {
	for key, value := range r {
		var err error

		if value == nil {
			err = rev.Delete([]byte(key))
		} else {
			err = rev.Put([]byte(key), value)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

type store map[string]partition
type partition struct {
	Metadata  []byte
	Revisions []revision
}

type revision struct {
	Revision int64
	Kvs      map[string][]byte
	Changes  map[string][]byte
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

		if err := mvccPartition.Create(partitionChangeset.metadata); err != nil {
			t.Fatalf("failed to create partition %s: %s", partitionName, err)
		}

		for _, transaction := range partitionChangeset.transactions {
			txn, err := mvccPartition.Begin(true)

			if err != nil {
				t.Fatalf("failed to apply transaction %#v: %s", transaction, err)
			}

			for _, op := range transaction.ops {
				currentPartition = op.apply(currentPartition)

				if err := op.applyToTransaction(txn); err != nil {
					t.Fatalf("failed to apply op %#v to partition %s: %s", op, partitionName, err)
				}
			}

			if transaction.shouldCommit {
				currentState[partitionName] = currentPartition
				err = txn.Commit()
			} else {
				err = txn.Rollback()
			}

			if err != nil {
				t.Fatalf("failed to apply transaction %#v: %s", transaction, err)
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
	txn, err := partition.Begin(false)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	defer txn.Rollback()

	view, err := txn.View(mvcc.RevisionOldest)

	if err == mvcc.ErrNoRevisions {
		return result
	} else if err != nil {
		t.Fatalf("error while trying to read oldest revision: %s", err)
	}

	for {
		result = append(result, revision{Kvs: getAllKVs(t, view), Changes: getAllChanges(t, view), Revision: view.Revision()})
		view, err = txn.View(view.Revision() + 1)

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
	for i := 0; i < 10; i++ {
		txn := transaction{ops: []op{}, shouldCommit: true}

		switch n := rand.Intn(100); {
		case rev > 0 && n < 5:
			txn = txn.compact(int64(rev))
		default:
			rev++
			txn = txn.newRevision(largeRevisionOp())
		}

		result.transactions = append(result.transactions, txn)
	}

	return result
}

func largeRevisionOp() revisionOp {
	result := revisionOp{}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", rand.Intn(100))

		switch n := rand.Intn(100); {
		case n < 50:
			result = result.delete([]byte(key))
		default:
			result = result.put([]byte(key), randomBytes())
		}
	}

	return result
}

func randomBytes() []byte {
	result := make([]byte, 20)
	crand.Read(result)

	return result
}
