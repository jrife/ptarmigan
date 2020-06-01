package storage_test

import (
	"fmt"
	"testing"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/plugins"
	"github.com/jrife/flock/storage/mvcc"
)

type tempStoreBuilder func(t testing.TB) storage.Store

func builder(plugin kv.Plugin) tempStoreBuilder {
	return func(t testing.TB) storage.Store {
		kvRootStore, err := plugins.Plugin("bbolt").NewTempRootStore()

		if err != nil {
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		t.Cleanup(func() { kvRootStore.Delete() })

		kvStore := kvRootStore.Store([]byte("test"))

		if err := kvStore.Create(); err != nil {
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		t.Cleanup(func() { kvStore.Delete() })

		mvccStore, err := mvcc.New(kvStore)

		if err != nil {
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		t.Cleanup(func() { mvccStore.Close() })

		return storage.New(storage.StoreConfig{
			Store: mvccStore,
		})
	}
}

func TestStorage(t *testing.T) {
	for _, plugin := range plugins.Plugins() {
		t.Run(fmt.Sprintf("Storage(%s)", plugin.Name()), func(t *testing.T) {
			testStorage(t, builder(plugin))
		})
	}
}
func testStorage(t *testing.T, builder tempStoreBuilder) {
	ptarmiganStore := builder(t)
	myReplicaStore := ptarmiganStore.ReplicaStore("my_replica_store")

	if err := myReplicaStore.Create([]byte{0, 1, 2}); err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	_, err := myReplicaStore.Txn(ptarmiganpb.RaftStatus{RaftIndex: 1}, ptarmiganpb.KVTxnRequest{
		Success: []*ptarmiganpb.KVRequestOp{
			{
				Request: &ptarmiganpb.KVRequestOp_RequestPut{
					RequestPut: &ptarmiganpb.KVPutRequest{
						Key:   []byte("abc"),
						Value: []byte("def"),
					},
				},
			},
		},
	})

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	response, err := myReplicaStore.Query(ptarmiganpb.KVQueryRequest{})

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	fmt.Printf("Query Response: %#v\n", response)
}
