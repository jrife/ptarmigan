package storage_test

import (
	"context"
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
	"github.com/jrife/flock/ptarmigan/storage/model"
	"github.com/jrife/flock/storage/mvcc"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
)

func replicaStoreModelDiff(replicaStore storage.ReplicaStore, model *model.ReplicaStoreModel) (string, error) {
	if index, err := replicaStore.Index(context.Background()); err != nil {
		return "", fmt.Errorf("could not retrieve index from replica store: %s", err)
	} else if index != model.Index() {
		return fmt.Sprintf("model index = %d, actual index = %d\n", model.Index(), index), nil
	}

	if leases, err := replicaStore.Leases(context.Background()); err != nil {
		return "", fmt.Errorf("could not retrieve leases from replica store: %s", err)
	} else {
		if diff := cmp.Diff(model.Leases(), leases); diff != "" {
			return fmt.Sprintf("leases don't match: %s", diff), nil
		}
	}

	oldestRevision, err := replicaStore.OldestRevision()

	if err != nil && err != mvcc.ErrNoRevisions {
		return "", fmt.Errorf("could not retrieve oldest revision from replica store: %s", err)
	}

	newestRevision, err := replicaStore.NewestRevision()

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
		if kvs, err := replicaStore.Query(context.Background(), ptarmiganpb.KVQueryRequest{Revision: revision}); err != nil {
			return "", fmt.Errorf("could not retrieve kvs from replica store at revision %d: %s", revision, err)
		} else {
			if diff := cmp.Diff(model.Query(ptarmiganpb.KVQueryRequest{Revision: revision}), kvs); diff != "" {
				return fmt.Sprintf("kvs don't match at revision %d: %s", revision, diff), nil
			}
		}
	}

	if changes, err := replicaStore.Changes(context.Background(), ptarmiganpb.KVWatchRequest{}, -1); err != nil {
		return "", fmt.Errorf("could not retrieve changes from replica store: %s", err)
	} else {
		if diff := cmp.Diff(model.Changes(ptarmiganpb.KVWatchRequest{}, -1), changes); diff != "" {
			return fmt.Sprintf("changes don't match: %s", diff), nil
		}
	}

	return "", nil
}

type TxnCommand ptarmiganpb.KVTxnRequest

func (command TxnCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	index, err := replicaStore.Index(context.Background())

	if err != nil {
		panic(err)
	}

	res, _ := replicaStore.Apply(index+1).Txn(context.Background(), ptarmiganpb.KVTxnRequest(command))
	return res
}

func (command TxnCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyTxn(state.(*model.ReplicaStoreModel).Index()+1, ptarmiganpb.KVTxnRequest(command))
	return state
}

func (command TxnCommand) PreCondition(state commands.State) bool {
	return true
}

func (command TxnCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	resp := state.(*model.ReplicaStoreModel).LastResponse().(ptarmiganpb.KVTxnResponse)
	diff := cmp.Diff(resp, result)

	if diff != "" {
		fmt.Printf("%s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command TxnCommand) String() string {
	return fmt.Sprintf("Txn(%#v)", command)
}

type CompactCommand int64

func (command CompactCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	index, err := replicaStore.Index(context.Background())

	if err != nil {
		panic(err)
	}

	// var newestRevision int64
	// var oldestRevision int64
	// revision := oldestRevision + int64(command)%(newestRevision-oldestRevision)
	replicaStore.Apply(index+1).Compact(context.Background(), int64(command))

	return sut
}

func (command CompactCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyCompact(state.(*model.ReplicaStoreModel).Index()+1, int64(command))
	return state
}

func (command CompactCommand) PreCondition(state commands.State) bool {
	if int64(command) == mvcc.RevisionNewest || int64(command) == mvcc.RevisionOldest {
		return true
	}

	if int64(command) < state.(*model.ReplicaStoreModel).OldestRevision() || int64(command) > state.(*model.ReplicaStoreModel).NewestRevision() {
		return false
	}

	return true
}

func (command CompactCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	model := state.(*model.ReplicaStoreModel)
	replicaStore := result.(storage.ReplicaStore)

	diff, err := replicaStoreModelDiff(replicaStore, model)

	if err != nil {
		fmt.Printf("%#v\n", err)

		return &gopter.PropResult{Status: gopter.PropFalse, Error: err}
	}

	if diff != "" {
		fmt.Printf("%s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command CompactCommand) String() string {
	return fmt.Sprintf("Compact(%#v)", command)
}

type QueryCommand ptarmiganpb.KVQueryRequest

func (command QueryCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	res, _ := replicaStore.Query(context.Background(), ptarmiganpb.KVQueryRequest(command))
	return res
}

func (command QueryCommand) NextState(state commands.State) commands.State {
	return state
}

func (command QueryCommand) PreCondition(state commands.State) bool {
	return true
}

func (command QueryCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(state.(*model.ReplicaStoreModel).Query(ptarmiganpb.KVQueryRequest(command)), result)

	if diff != "" {
		fmt.Printf("%s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command QueryCommand) String() string {
	return fmt.Sprintf("Query(%#v)", command)
}
