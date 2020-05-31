package ptarmigan_test

import (
	"fmt"
	"testing"

	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage/ptarmigan"
	"github.com/jrife/flock/storage/kv/plugins"
	"github.com/jrife/flock/storage/mvcc"
)

func TestPtarmiganStorage(t *testing.T) {
	kvRootStore, err := plugins.Plugin("bbolt").NewTempRootStore()

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	defer kvRootStore.Delete()

	kvStore := kvRootStore.Store([]byte("test"))

	if err := kvStore.Create(); err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	defer kvStore.Delete()

	mvccStore, err := mvcc.New(kvStore)

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	defer mvccStore.Close()

	ptarmiganStore := ptarmigan.New(ptarmigan.StoreConfig{
		Store: mvccStore,
	})

	myReplicaStore := ptarmiganStore.ReplicaStore("my_replica_store")

	if err := myReplicaStore.Create([]byte{0, 1, 2}); err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	response, err := myReplicaStore.Txn(ptarmiganpb.RaftStatus{RaftIndex: 1}, ptarmiganpb.KVTxnRequest{})

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	fmt.Printf("Txn Response: %#v\n", response)
}
