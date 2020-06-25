package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/plugins"
	"github.com/jrife/flock/storage/mvcc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type tempStoreBuilder func(t testing.TB) (storage.Store, func())

func builder(plugin kv.Plugin) tempStoreBuilder {
	return func(t testing.TB) (storage.Store, func()) {
		kvRootStore, err := plugins.Plugin("bbolt").NewTempRootStore()

		if err != nil {
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		kvStore := kvRootStore.Store([]byte("test"))

		if err := kvStore.Create(); err != nil {
			kvRootStore.Delete()
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		mvccStore, err := mvcc.New(kvStore)

		if err != nil {
			kvStore.Delete()
			kvRootStore.Delete()
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		cleanup := func() {
			mvccStore.Close()
			kvStore.Delete()
			kvRootStore.Delete()
		}

		atom := zap.NewAtomicLevel()
		logger := zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zapcore.Lock(os.Stdout),
			atom,
		))
		atom.SetLevel(zap.InfoLevel)

		return storage.New(storage.StoreConfig{
			Store:  mvccStore,
			Logger: logger,
		}), cleanup
	}
}

var errAny = errors.New("")

func TestStorage(t *testing.T) {
	for _, plugin := range plugins.Plugins() {
		t.Run(fmt.Sprintf("Storage(%s)", plugin.Name()), func(t *testing.T) {
			testStorage(builder(plugin), t)
		})
	}
}

func testStorage(builder tempStoreBuilder, t *testing.T) {
	t.Run("Store", func(t *testing.T) { testStore(builder, t) })
	t.Run("ReplicaStore", func(t *testing.T) { testReplicaStore(builder, t) })
}

func testStore(builder tempStoreBuilder, t *testing.T) {
	t.Run("ReplicaStores", func(t *testing.T) { testStoreReplicaStores(builder, t) })
}

func testStoreReplicaStores(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStores []string
		start         string
		limit         int
		result        []string
	}{
		"empty": {
			replicaStores: []string{},
			start:         "",
			limit:         -1,
			result:        []string{},
		},
		"empty-non-empty-start": {
			replicaStores: []string{},
			start:         "abc",
			limit:         -1,
			result:        []string{},
		},
		"not-empty-all": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "",
			limit:         -1,
			result:        []string{"abc", "def", "ghi", "jkl"},
		},
		"not-empty-first-half-1": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "",
			limit:         2,
			result:        []string{"abc", "def"},
		},
		"not-empty-first-half-2": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "a",
			limit:         2,
			result:        []string{"abc", "def"},
		},
		"not-empty-last-half-1": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "def",
			limit:         2,
			result:        []string{"ghi", "jkl"},
		},
		"not-empty-last-half-2": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "eee",
			limit:         2,
			result:        []string{"ghi", "jkl"},
		},
		"not-empty-middle-half-1": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "abc",
			limit:         2,
			result:        []string{"def", "ghi"},
		},
		"not-empty-middle-half-2": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "ccc",
			limit:         2,
			result:        []string{"def", "ghi"},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			for i, replicaStore := range testCase.replicaStores {
				ptarmiganStore.ReplicaStore(replicaStore).Create(context.Background(), []byte(fmt.Sprintf("metadata-%d", i)))
			}

			replicaStores, err := ptarmiganStore.ReplicaStores(context.Background(), testCase.start, testCase.limit)

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			diff := cmp.Diff(testCase.result, replicaStores)

			if diff != "" {
				t.Fatalf(diff)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStores(context.Background(), testCase.start, testCase.limit)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStore(builder tempStoreBuilder, t *testing.T) {
	t.Run("Create", func(t *testing.T) { testReplicaStoreCreate(builder, t) })
	t.Run("Delete", func(t *testing.T) { testReplicaStoreDelete(builder, t) })
	t.Run("Metadata", func(t *testing.T) { testReplicaStoreMetadata(builder, t) })
	t.Run("Index", func(t *testing.T) { testReplicaStoreIndex(builder, t) })
	t.Run("ApplyTxn", func(t *testing.T) { testReplicaStoreApplyTxn(builder, t) })
	t.Run("ApplyCreateLease", func(t *testing.T) { testReplicaStoreApplyCreateLease(builder, t) })
	t.Run("ApplyRevokeLease", func(t *testing.T) { testReplicaStoreApplyRevokeLease(builder, t) })
	t.Run("ApplyCompact", func(t *testing.T) { testReplicaStoreApplyCompact(builder, t) })
	t.Run("Query", func(t *testing.T) { testReplicaStoreQuery(builder, t) })
	t.Run("Changes", func(t *testing.T) { testReplicaStoreChanges(builder, t) })
	t.Run("Leases", func(t *testing.T) { testReplicaStoreLeases(builder, t) })
	t.Run("GetLease", func(t *testing.T) { testReplicaStoreGetLease(builder, t) })
	t.Run("NewestRevision", func(t *testing.T) { testReplicaStoreNewestRevision(builder, t) })
	t.Run("OldestRevision", func(t *testing.T) { testReplicaStoreOldestRevision(builder, t) })
	t.Run("Snapshot", func(t *testing.T) { testReplicaStoreSnapshot(builder, t) })
	t.Run("ApplySnapshot", func(t *testing.T) { testReplicaStoreApplySnapshot(builder, t) })
}

func testReplicaStoreCreate(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore string
		metadata     []byte
		err          error
	}{
		"empty-metadata": {
			replicaStore: "abc",
			metadata:     []byte{},
		},
		"non-empty-metadata": {
			replicaStore: "abc",
			metadata:     []byte("asdf"),
		},
		"nil-metadata": {
			replicaStore: "abc",
			metadata:     nil,
		},
		"empty-name": {
			replicaStore: "",
			metadata:     []byte{},
			err:          errAny,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			createErr := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), testCase.metadata)

			if testCase.err == errAny {
				if createErr == nil {
					t.Fatalf("expected err to not be nil, got nil")
				}
			} else if testCase.err != createErr {
				t.Fatalf("expected err to be %#v, got %#v", testCase.err, createErr)
			}

			metadata, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Metadata(context.Background())

			if createErr != nil {
				if err == nil {
					t.Fatalf("expected err to not be nil, got %#v", err)
				}
			} else if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			if bytes.Compare(metadata, testCase.metadata) != 0 {
				t.Fatalf("expected metadata to be %#v, got %#v", testCase.metadata, metadata)
			}

			ptarmiganStore.Close()

			err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), testCase.metadata)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreDelete(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore string
		create       bool
		err          error
	}{
		"delete-existing": {
			replicaStore: "abc",
			create:       true,
		},
		"delete-not-existing": {
			replicaStore: "abc",
			create:       false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.create {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Delete(context.Background())

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Metadata(context.Background())

			if err != storage.ErrNoSuchReplicaStore {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrNoSuchReplicaStore, err)
			}

			ptarmiganStore.Close()

			err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Delete(context.Background())

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreMetadata(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore string
		create       bool
		err          error
	}{
		"existing": {
			replicaStore: "abc",
			create:       true,
		},
		"not-existing": {
			replicaStore: "abc",
			create:       false,
			err:          storage.ErrNoSuchReplicaStore,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.create {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			metadata, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Metadata(context.Background())

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			if err == nil && bytes.Compare(metadata, []byte("metadata")) != 0 {
				t.Fatalf("expected metadata to be %#v, got %#v", []byte("metadata"), metadata)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Metadata(context.Background())

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreIndex(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore string
		create       bool
		err          error
	}{
		"existing": {
			replicaStore: "abc",
			create:       true,
		},
		"not-existing": {
			replicaStore: "abc",
			create:       false,
			err:          storage.ErrNoSuchReplicaStore,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.create {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			index, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Index(context.Background())

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			if err == nil && index != 0 {
				t.Fatalf("expected index to be %#v, got %#v", 0, index)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Index(context.Background())

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreApplyTxn(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		createLeases       bool
		index              uint64
		txn                ptarmiganpb.KVTxnRequest
		err                error
	}{
		"no-such-replica-store": {
			replicaStore:       "abc",
			createReplicaStore: false,
			createLeases:       false,
			index:              3,
			err:                storage.ErrNoSuchReplicaStore,
		},
		"index-not-monotonic": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createLeases:       true,
			index:              1,
			err:                storage.ErrIndexNotMonotonic,
		},
		"no-such-lease": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createLeases:       true,
			index:              3,
			txn: ptarmiganpb.KVTxnRequest{
				Success: []ptarmiganpb.KVRequestOp{
					{
						Request: &ptarmiganpb.KVRequestOp_RequestPut{
							RequestPut: &ptarmiganpb.KVPutRequest{
								Key:   []byte("a"),
								Value: []byte("c"),
								Lease: 44,
							},
						},
					},
				},
			},
			err: storage.ErrNoSuchLease,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}

				if testCase.createLeases {
					_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(1).CreateLease(context.Background(), 200)

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(2).CreateLease(context.Background(), 200)

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}
				}
			}

			_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(testCase.index).Txn(context.Background(), testCase.txn)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(testCase.index).Txn(context.Background(), testCase.txn)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreApplyCompact(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		createRevisions    bool
		compactRevisions   bool
		index              uint64
		revision           int64
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			index:        0,
			err:          storage.ErrNoSuchReplicaStore,
		},
		"index-not-monotonic": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    true,
			index:              1,
			err:                storage.ErrIndexNotMonotonic,
		},
		"no-revisions": {
			replicaStore:       "abc",
			createReplicaStore: true,
			index:              1,
			revision:           1,
			err:                mvcc.ErrNoRevisions,
		},
		"revision-too-high": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    true,
			index:              3,
			revision:           3,
			err:                mvcc.ErrRevisionTooHigh,
		},
		"compacted": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    true,
			compactRevisions:   true,
			index:              4,
			revision:           1,
			err:                mvcc.ErrCompacted,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}

				if testCase.createRevisions {
					_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(1).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("b"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(2).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("c"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					if testCase.compactRevisions {
						err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(3).Compact(context.Background(), 2)

						if err != nil {
							t.Fatalf("expcted err to be nil, got %#v", err)
						}
					}
				}
			}

			err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(testCase.index).Compact(context.Background(), testCase.revision)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(testCase.index).Compact(context.Background(), testCase.revision)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreApplyCreateLease(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		index              uint64
		ttl                int64
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			index:        0,
			ttl:          1,
			err:          storage.ErrNoSuchReplicaStore,
		},
		"index-not-monotonic": {
			replicaStore:       "abc",
			createReplicaStore: true,
			index:              0,
			err:                storage.ErrIndexNotMonotonic,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(testCase.index).CreateLease(context.Background(), testCase.ttl)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(testCase.index).CreateLease(context.Background(), testCase.ttl)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreApplyRevokeLease(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		index              uint64
		id                 int64
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			err:          storage.ErrNoSuchReplicaStore,
		},
		"index-not-monotonic": {
			replicaStore:       "abc",
			createReplicaStore: true,
			index:              0,
			err:                storage.ErrIndexNotMonotonic,
		},
		"existing": {
			replicaStore:       "abc",
			createReplicaStore: true,
			index:              2,
			id:                 1,
		},
		"not-existing": {
			replicaStore:       "abc",
			createReplicaStore: true,
			index:              2,
			id:                 30,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}

				_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(1).CreateLease(context.Background(), 1)

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(testCase.index).RevokeLease(context.Background(), testCase.id)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(testCase.index).RevokeLease(context.Background(), testCase.id)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreQuery(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		createRevisions    bool
		compactRevisions   bool
		query              ptarmiganpb.KVQueryRequest
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			err:          storage.ErrNoSuchReplicaStore,
		},
		"revision-too-high": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    true,
			query:              ptarmiganpb.KVQueryRequest{Revision: 3},
			err:                mvcc.ErrRevisionTooHigh,
		},
		"compacted": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    true,
			compactRevisions:   true,
			query:              ptarmiganpb.KVQueryRequest{Revision: 1},
			err:                mvcc.ErrCompacted,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}

				if testCase.createRevisions {
					_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(1).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("b"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(2).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("c"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					if testCase.compactRevisions {
						err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(3).Compact(context.Background(), 2)

						if err != nil {
							t.Fatalf("expcted err to be nil, got %#v", err)
						}
					}
				}
			}

			_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Query(context.Background(), testCase.query)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Query(context.Background(), testCase.query)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreChanges(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		createRevisions    bool
		compactRevisions   bool
		query              ptarmiganpb.KVWatchRequest
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			err:          storage.ErrNoSuchReplicaStore,
		},
		"revision-too-high": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    true,
			query:              ptarmiganpb.KVWatchRequest{Start: ptarmiganpb.KVWatchCursor{Revision: 3}},
			err:                mvcc.ErrRevisionTooHigh,
		},
		"compacted": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    true,
			compactRevisions:   true,
			query:              ptarmiganpb.KVWatchRequest{Start: ptarmiganpb.KVWatchCursor{Revision: 1}},
			err:                mvcc.ErrCompacted,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}

				if testCase.createRevisions {
					_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(1).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("b"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(2).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("c"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					if testCase.compactRevisions {
						err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(3).Compact(context.Background(), 2)

						if err != nil {
							t.Fatalf("expcted err to be nil, got %#v", err)
						}
					}
				}
			}

			_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Changes(context.Background(), testCase.query, 1)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Changes(context.Background(), testCase.query, 1)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreLeases(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			err:          storage.ErrNoSuchReplicaStore,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Leases(context.Background())

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Leases(context.Background())

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreGetLease(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		createLeases       bool
		id                 int64
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			err:          storage.ErrNoSuchReplicaStore,
		},
		"no-such-lease": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createLeases:       true,
			id:                 3,
			err:                storage.ErrNoSuchLease,
		},
		"lease-exists": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createLeases:       true,
			id:                 2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}

				if testCase.createLeases {
					_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(1).CreateLease(context.Background(), 200)

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(2).CreateLease(context.Background(), 200)

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}
				}
			}

			_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).GetLease(context.Background(), testCase.id)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).GetLease(context.Background(), testCase.id)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreNewestRevision(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		createRevisions    bool
		compactRevisions   bool
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			err:          storage.ErrNoSuchReplicaStore,
		},
		"no-revisions": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    false,
			err:                mvcc.ErrNoRevisions,
		},
		"some-revisions": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    true,
			compactRevisions:   true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}

				if testCase.createRevisions {
					_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(1).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("b"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(2).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("c"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					if testCase.compactRevisions {
						err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(3).Compact(context.Background(), 2)

						if err != nil {
							t.Fatalf("expcted err to be nil, got %#v", err)
						}
					}
				}
			}

			_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).NewestRevision(context.Background())

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).NewestRevision(context.Background())

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreOldestRevision(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		createRevisions    bool
		compactRevisions   bool
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			err:          storage.ErrNoSuchReplicaStore,
		},
		"no-revisions": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    false,
			err:                mvcc.ErrNoRevisions,
		},
		"some-revisions": {
			replicaStore:       "abc",
			createReplicaStore: true,
			createRevisions:    true,
			compactRevisions:   true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}

				if testCase.createRevisions {
					_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(1).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("b"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(2).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
						Success: []ptarmiganpb.KVRequestOp{
							{
								Request: &ptarmiganpb.KVRequestOp_RequestPut{
									RequestPut: &ptarmiganpb.KVPutRequest{
										Key:   []byte("a"),
										Value: []byte("c"),
									},
								},
							},
						},
					})

					if err != nil {
						t.Fatalf("expcted err to be nil, got %#v", err)
					}

					if testCase.compactRevisions {
						err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Apply(3).Compact(context.Background(), 2)

						if err != nil {
							t.Fatalf("expcted err to be nil, got %#v", err)
						}
					}
				}
			}

			_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).OldestRevision(context.Background())

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).OldestRevision(context.Background())

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreSnapshot(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		err                error
	}{
		"no-such-replica-store": {
			replicaStore: "abc",
			err:          storage.ErrNoSuchReplicaStore,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			if testCase.createReplicaStore {
				err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			_, err := ptarmiganStore.ReplicaStore(testCase.replicaStore).Snapshot(context.Background())

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			ptarmiganStore.Close()

			_, err = ptarmiganStore.ReplicaStore(testCase.replicaStore).Snapshot(context.Background())

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}

func testReplicaStoreApplySnapshot(builder tempStoreBuilder, t *testing.T) {
	// Need to make sure it's ok for the replica store not to exist (should create it)
	testCases := map[string]struct {
		replicaStore       string
		createReplicaStore bool
		err                error
	}{
		"no-such-replica-store": {
			replicaStore:       "abc",
			createReplicaStore: false,
		},
		"replica-store-exists": {
			replicaStore:       "abc",
			createReplicaStore: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			sourceStore, cleanupSource := builder(t)
			destStore, cleanupDest := builder(t)
			defer cleanupSource()
			defer cleanupDest()

			err := sourceStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			_, err = sourceStore.ReplicaStore(testCase.replicaStore).Apply(1).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
				Success: []ptarmiganpb.KVRequestOp{
					{
						Request: &ptarmiganpb.KVRequestOp_RequestPut{
							RequestPut: &ptarmiganpb.KVPutRequest{
								Key:   []byte("a"),
								Value: []byte("b"),
							},
						},
					},
				},
			})

			if err != nil {
				t.Fatalf("expcted err to be nil, got %#v", err)
			}

			_, err = sourceStore.ReplicaStore(testCase.replicaStore).Apply(2).Txn(context.Background(), ptarmiganpb.KVTxnRequest{
				Success: []ptarmiganpb.KVRequestOp{
					{
						Request: &ptarmiganpb.KVRequestOp_RequestPut{
							RequestPut: &ptarmiganpb.KVPutRequest{
								Key:   []byte("a"),
								Value: []byte("c"),
							},
						},
					},
				},
			})

			if err != nil {
				t.Fatalf("expcted err to be nil, got %#v", err)
			}

			if testCase.createReplicaStore {
				err := destStore.ReplicaStore(testCase.replicaStore).Create(context.Background(), []byte("metadata"))

				if err != nil {
					t.Fatalf("expected err to be nil, got %#v", err)
				}
			}

			snap, err := sourceStore.ReplicaStore(testCase.replicaStore).Snapshot(context.Background())

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			err = destStore.ReplicaStore(testCase.replicaStore).ApplySnapshot(context.Background(), snap)

			if testCase.err == errAny {
				if err == nil {
					t.Fatalf("expected err not to be nil, got nil")
				}
			} else if err != testCase.err {
				t.Fatalf("expcted err to be %#v, got %#v", testCase.err, err)
			}

			destStore.Close()

			err = destStore.ReplicaStore(testCase.replicaStore).ApplySnapshot(context.Background(), snap)

			if err != storage.ErrClosed {
				t.Fatalf("expected err to be %#v, got %#v", storage.ErrClosed, err)
			}
		})
	}
}
