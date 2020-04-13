package mvcc_test

import (
	"testing"

	"github.com/jrife/ptarmigan/flock/server/flockpb"

	"github.com/jrife/ptarmigan/flock/storage/mvcc"
)

func TestMVCC(t *testing.T) {
	var store mvcc.IStore

	replicaStoreA := store.ReplicaStore("A")

	if err := replicaStoreA.Create([]byte{}); err != nil {
		t.Fatalf("Could not create store A: %s", err.Error())
	}

	revision, err := replicaStoreA.NewRevision(flockpb.RaftStatus{})

	if err != nil {
		t.Fatalf("Could not create new revision: %s", err.Error())
	}

	if err := revision.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("Could not update key a: %s", err.Error())
	}

	if err := revision.Commit(); err != nil {
		t.Fatalf("Could not commit revision: %s", err.Error())
	}
}
