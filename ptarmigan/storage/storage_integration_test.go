// +build integration

package storage_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
	"github.com/jrife/flock/ptarmigan/storage/model"
	command_gen "github.com/jrife/flock/ptarmigan/storage/model/gen"
	"github.com/jrife/flock/storage/kv/plugins"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
)

func randomBytes() []byte {
	r := make([]byte, 100)
	rand.Read(r)
	return r
}

func TestStorageIntegration(t *testing.T) {
	for _, plugin := range plugins.Plugins() {
		t.Run(fmt.Sprintf("StorageIntegration(%s)", plugin.Name()), func(t *testing.T) {
			testStorageIntegration(builder(plugin), t)
		})
	}
}

func testStorageIntegration(builder tempStoreBuilder, t *testing.T) {
	t.Run("ReplicaStore", func(t *testing.T) { testReplicaStoreIntegration(builder, t) })
}

func testReplicaStoreIntegration(builder tempStoreBuilder, t *testing.T) {
	t.Run("System", func(t *testing.T) { testReplicaStoreSystem(builder, t) })
	t.Run("SystemSnapshot", func(t *testing.T) { testReplicaStoreSystemSnapshot(builder, t) })
	t.Run("LargeSnapshot", func(t *testing.T) { testReplicaStoreLargeSnapshot(builder, t) })
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
			return command_gen.CommandsOne(0, state.(*model.ReplicaStoreModel))
		},
	}

	parameters := gopter.DefaultTestParametersWithSeed(1234)
	parameters.MinSuccessfulTests = 1000
	parameters.MaxSize = 30
	properties := gopter.NewProperties(parameters)
	properties.Property("", commands.Prop(cbCommands))
	properties.TestingRun(t)
}

type replicaStoreSetWithCleanup struct {
	replicaStores []storage.ReplicaStore
	cleanup       func()
}

func (replicaStoreSet replicaStoreSetWithCleanup) Get(i int) storage.ReplicaStore {
	return replicaStoreSet.replicaStores[i]
}

func testReplicaStoreSystemSnapshot(builder tempStoreBuilder, t *testing.T) {
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

			return replicaStoreSetWithCleanup{
				replicaStores: []storage.ReplicaStore{replicaStoreA, replicaStoreB},
				cleanup: func() {
					cleanupA()
					cleanupB()
				},
			}
		},
		DestroySystemUnderTestFunc: func(sut commands.SystemUnderTest) {
			sut.(replicaStoreSetWithCleanup).cleanup()
		},
		InitialStateGen: gopter.CombineGens().Map(func([]interface{}) []model.ReplicaStoreModel {
			return []model.ReplicaStoreModel{*model.NewReplicaStoreModel(), *model.NewReplicaStoreModel()}
		}),
		InitialPreConditionFunc: func(state commands.State) bool {
			return true
		},
		GenCommandFunc: func(state commands.State) gopter.Gen {
			return command_gen.Commands(state.([]model.ReplicaStoreModel))
		},
	}

	parameters := gopter.DefaultTestParametersWithSeed(1234)
	parameters.MinSuccessfulTests = 1000
	parameters.MaxSize = 30
	properties := gopter.NewProperties(parameters)
	properties.Property("", commands.Prop(cbCommands))
	properties.TestingRun(t)
}

func testReplicaStoreLargeSnapshot(builder tempStoreBuilder, t *testing.T) {
	storeA, cleanupA := builder(t)
	storeB, cleanupB := builder(t)
	replicaStoreA := storeA.ReplicaStore("test")
	replicaStoreB := storeB.ReplicaStore("test")
	replicaStoreModelA := model.NewReplicaStoreModel()

	defer cleanupA()
	defer cleanupB()

	if err := replicaStoreA.Create(context.Background(), []byte("abc")); err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	if err := replicaStoreB.Create(context.Background(), []byte("def")); err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	// write 1k keys in batches of 10
	for i := 0; i < 100; i++ {
		txnRequest := ptarmiganpb.KVTxnRequest{
			Success: []ptarmiganpb.KVRequestOp{},
		}

		for j := 0; j < 10; j++ {
			txnRequest.Success = append(txnRequest.Success, ptarmiganpb.KVRequestOp{
				Request: &ptarmiganpb.KVRequestOp_RequestPut{
					RequestPut: &ptarmiganpb.KVPutRequest{
						Key:   randomBytes(),
						Value: randomBytes(),
					},
				},
			})
		}

		replicaStoreModelA.ApplyTxn(replicaStoreModelA.Index()+1, txnRequest)
		_, err := replicaStoreA.Apply(replicaStoreModelA.Index()).Txn(context.Background(), txnRequest)

		if err != nil {
			t.Fatalf("expected err to be nil, got %#v", err)
		}
	}

	// apply snapshot and make sure replica store B looks the same as A
	snap, err := replicaStoreA.Snapshot(context.Background())

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	err = replicaStoreB.ApplySnapshot(context.Background(), snap)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	diff, err := command_gen.ReplicaStoreModelDiff(replicaStoreB, replicaStoreModelA)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	if diff != "" {
		t.Fatalf(diff)
	}

	metaA, err := replicaStoreA.Metadata(context.Background())

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	metaB, err := replicaStoreB.Metadata(context.Background())

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	diff = cmp.Diff(metaA, metaB)

	if diff != "" {
		t.Fatalf(diff)
	}
}
