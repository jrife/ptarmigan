package gen

import (
	"context"
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
	"github.com/jrife/flock/ptarmigan/storage/model"
	"github.com/jrife/flock/storage/mvcc"
)

// ReplicaStoreModelDiff returns the diff between the actual replica store and a model.
// Returns an empty string if they are identical.
func ReplicaStoreModelDiff(replicaStore storage.ReplicaStore, model *model.ReplicaStoreModel) (string, error) {
	if index, err := replicaStore.Index(context.Background()); err != nil {
		return "", fmt.Errorf("could not retrieve index from replica store: %s", err)
	} else if index != model.Index() {
		return fmt.Sprintf("model index = %d, actual index = %d\n", model.Index(), index), nil
	}

	leases, err := replicaStore.Leases(context.Background())

	if err != nil {
		return "", fmt.Errorf("could not retrieve leases from replica store: %s", err)
	}

	if diff := cmp.Diff(model.Leases(), leases); diff != "" {
		return fmt.Sprintf("leases don't match: %s", diff), nil
	}

	oldestRevision, err := replicaStore.OldestRevision(context.Background())

	if err != nil && err != mvcc.ErrNoRevisions {
		return "", fmt.Errorf("could not retrieve oldest revision from replica store: %s", err)
	}

	newestRevision, err := replicaStore.NewestRevision(context.Background())

	if err != nil && err != mvcc.ErrNoRevisions {
		return "", fmt.Errorf("could not retrieve newest revision from replica store: %s", err)
	}

	if oldestRevision != model.OldestRevision() {
		return fmt.Sprintf("model oldest revision = %d, actual oldest revision = %d\n", model.OldestRevision(), oldestRevision), nil
	}

	if newestRevision != model.NewestRevision() {
		return fmt.Sprintf("model newest revision = %d, actual newest revision = %d\n", model.NewestRevision(), newestRevision), nil
	}

	for revision := oldestRevision; revision <= newestRevision; revision++ {
		kvs, err := replicaStore.Query(context.Background(), ptarmiganpb.KVQueryRequest{Revision: revision})

		if err != nil {
			return "", fmt.Errorf("could not retrieve kvs from replica store at revision %d: %s", revision, err)
		}

		if diff := cmp.Diff(model.Query(ptarmiganpb.KVQueryRequest{Revision: revision}), kvs); diff != "" {
			return fmt.Sprintf("kvs don't match at revision %d: %s", revision, diff), nil
		}
	}

	changes, err := replicaStore.Changes(context.Background(), ptarmiganpb.KVWatchRequest{Start: ptarmiganpb.KVWatchCursor{Revision: mvcc.RevisionOldest}}, -1)

	if err != nil {
		return "", fmt.Errorf("could not retrieve changes from replica store: %s", err)
	}

	if diff := cmp.Diff(model.Changes(ptarmiganpb.KVWatchRequest{Start: ptarmiganpb.KVWatchCursor{Revision: mvcc.RevisionOldest}}, -1), changes); diff != "" {
		return fmt.Sprintf("changes don't match: %s", diff), nil
	}

	return "", nil
}

func indexFromRange(seed int64, min int64, max int64) int64 {
	return min + seed%(max-min)
}
