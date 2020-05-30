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

	revision, err := myReplicaStore.NewRevision(ptarmiganpb.RaftStatus{RaftIndex: 1})

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	defer revision.Rollback()

	kv, err := revision.Get([]byte("a"))

	if err != nil {
		t.Fatalf("expected err to be nil, got %#v", err)
	}

	fmt.Printf("KV: %#v\n", kv)
}
