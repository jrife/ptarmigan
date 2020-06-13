package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
	"github.com/jrife/flock/ptarmigan/storage/model"
	command_gen "github.com/jrife/flock/ptarmigan/storage/model/gen"
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/plugins"
	"github.com/jrife/flock/storage/mvcc"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
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
		atom.SetLevel(zap.DebugLevel)

		return storage.New(storage.StoreConfig{
			Store:  mvccStore,
			Logger: logger,
		}), cleanup
	}
}

type transaction struct {
	Txn         *ptarmiganpb.KVTxnRequest
	Compact     *int64
	CreateLease *int64
	RevokeLease *int64
}

type transactionResponse struct {
	Txn            *ptarmiganpb.KVTxnResponse
	TxnErr         error
	CompactErr     error
	CreateLease    *ptarmiganpb.Lease
	CreateLeaseErr error
	RevokeLeaseErr error
}

var errAny = errors.New("")

func normalizeLease(lease ptarmiganpb.Lease) ptarmiganpb.Lease {
	return ptarmiganpb.Lease{
		ID:         lease.ID,
		GrantedTTL: lease.GrantedTTL,
	}
}

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
		})
	}
}

func testReplicaStore(builder tempStoreBuilder, t *testing.T) {
	t.Run("System", func(t *testing.T) { testReplicaStoreSystem(builder, t) })
	t.Run("SystemSnapshot", func(t *testing.T) { testReplicaStoreSystemSnapshot(builder, t) })
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

func testReplicaStoreSystem(builder tempStoreBuilder, t *testing.T) {
	type replicaStoreWithCleanup struct {
		storage.ReplicaStore
		cleanup func()
	}

	var cbCommands = &commands.ProtoCommands{
		NewSystemUnderTestFunc: func(initialState commands.State) commands.SystemUnderTest {
			store, cleanup := builder(t)
			replicaStore := store.ReplicaStore("test")

			if err := replicaStore.Create(context.Background(), []byte{}); err != nil {
				panic(err)
			}

			return &replicaStoreWithCleanup{ReplicaStore: replicaStore, cleanup: cleanup}
		},
		DestroySystemUnderTestFunc: func(sut commands.SystemUnderTest) {
			sut.(*replicaStoreWithCleanup).cleanup()
		},
		InitialStateGen: gopter.CombineGens().Map(func([]interface{}) *model.ReplicaStoreModel {
			return model.NewReplicaStoreModel()
		}),
		InitialPreConditionFunc: func(state commands.State) bool {
			return true
		},
		GenCommandFunc: func(state commands.State) gopter.Gen {
			return command_gen.Commands(state.(*model.ReplicaStoreModel))
		},
	}

	parameters := gopter.DefaultTestParametersWithSeed(1234)
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)
	properties.Property("", commands.Prop(cbCommands))
	properties.TestingRun(t)
}

func testReplicaStoreSystemSnapshot(builder tempStoreBuilder, t *testing.T) {
	type replicaStoreABWithCleanup struct {
		A       storage.ReplicaStore
		B       storage.ReplicaStore
		cleanup func()
	}

	type stateWithInitialCommands struct {
		model           *model.ReplicaStoreModel
		initialCommands []commands.Command
	}

	var cbCommands = &commands.ProtoCommands{
		NewSystemUnderTestFunc: func(initialState commands.State) commands.SystemUnderTest {
			storeA, cleanupA := builder(t)
			storeB, cleanupB := builder(t)

			replicaStoreA := storeA.ReplicaStore("test")
			replicaStoreB := storeB.ReplicaStore("test")

			if err := replicaStoreA.Create(context.Background(), []byte{}); err != nil {
				panic(err)
			}

			if err := replicaStoreB.Create(context.Background(), []byte{}); err != nil {
				panic(err)
			}

			return &replicaStoreABWithCleanup{
				A: replicaStoreA,
				B: replicaStoreB,
				cleanup: func() {
					cleanupA()
					cleanupB()
				},
			}
		},
		DestroySystemUnderTestFunc: func(sut commands.SystemUnderTest) {
			sut.(*replicaStoreWithCleanup).cleanup()
		},
		InitialStateGen: gopter.CombineGens().Map(func([]interface{}) *model.ReplicaStoreModel {
			c := []commands.Command{}
			m := &model.ReplicaStoreModel{}
			m = c[0].NextState(m).(*model.ReplicaStoreModel)

			return model.NewReplicaStoreModel()
		}),
		InitialPreConditionFunc: func(state commands.State) bool {
			return true
		},
		GenCommandFunc: func(state commands.State) gopter.Gen {
			return command_gen.Commands(state.(*model.ReplicaStoreModel))
		},
	}

	parameters := gopter.DefaultTestParametersWithSeed(1234)
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)
	properties.Property("", commands.Prop(cbCommands))
	properties.TestingRun(t)
}

func testReplicaStoreCreate(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreDelete(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreMetadata(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreIndex(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreApplyTxn(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreApplyCompact(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreApplyCreateLease(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreApplyRevokeLease(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreQuery(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreChanges(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreLeases(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreGetLease(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreNewestRevision(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreOldestRevision(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreApplySnapshot(builder tempStoreBuilder, t *testing.T) {
}

func testReplicaStoreSnapshot(builder tempStoreBuilder, t *testing.T) {
}
