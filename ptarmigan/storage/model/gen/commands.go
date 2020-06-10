package gen

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

	leases, err := replicaStore.Leases(context.Background())

	if err != nil {
		return "", fmt.Errorf("could not retrieve leases from replica store: %s", err)
	}

	if diff := cmp.Diff(model.Leases(), leases); diff != "" {
		return fmt.Sprintf("leases don't match: %s", diff), nil
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
		kvs, err := replicaStore.Query(context.Background(), ptarmiganpb.KVQueryRequest{Revision: revision})

		if err != nil {
			return "", fmt.Errorf("could not retrieve kvs from replica store at revision %d: %s", revision, err)
		}

		if diff := cmp.Diff(model.Query(ptarmiganpb.KVQueryRequest{Revision: revision}), kvs); diff != "" {
			fmt.Printf("expected: %#v\nactual: %#v\n", model.Query(ptarmiganpb.KVQueryRequest{Revision: revision}).Kvs[0], kvs.Kvs[0])
			return fmt.Sprintf("kvs don't match at revision %d: %s", revision, diff), nil
		}
	}

	changes, err := replicaStore.Changes(context.Background(), ptarmiganpb.KVWatchRequest{}, -1)

	if err != nil {
		return "", fmt.Errorf("could not retrieve changes from replica store: %s", err)
	}

	if diff := cmp.Diff(model.Changes(ptarmiganpb.KVWatchRequest{}, -1), changes); diff != "" {
		fmt.Printf("model %#v\n", model.Changes(ptarmiganpb.KVWatchRequest{}, -1))
		fmt.Printf("actual %#v\n", changes)

		return fmt.Sprintf("changes don't match: %s", diff), nil
	}

	return "", nil
}

func indexFromRange(seed int64, min int64, max int64) int64 {
	return min + seed%(max-min)
}

type txnCommand ptarmiganpb.KVTxnRequest

func (command txnCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	index, err := replicaStore.Index(context.Background())

	if err != nil {
		panic(err)
	}

	res, _ := replicaStore.Apply(index+1).Txn(context.Background(), ptarmiganpb.KVTxnRequest(command))
	return res
}

func (command txnCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyTxn(state.(*model.ReplicaStoreModel).Index()+1, ptarmiganpb.KVTxnRequest(command))
	return state
}

func (command txnCommand) PreCondition(state commands.State) bool {
	return true
}

func (command txnCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	resp := state.(*model.ReplicaStoreModel).LastResponse().(ptarmiganpb.KVTxnResponse)
	diff := cmp.Diff(resp, result)

	if diff != "" {
		fmt.Printf("%s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command txnCommand) String() string {
	return fmt.Sprintf("Txn(%#v)", command)
}

type compactCommand int64

func (command compactCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	index, err := replicaStore.Index(context.Background())

	if err != nil {
		panic(err)
	}

	replicaStore.Apply(index+1).Compact(context.Background(), int64(command))

	return sut
}

func (command compactCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyCompact(state.(*model.ReplicaStoreModel).Index()+1, int64(command))
	return state
}

func (command compactCommand) PreCondition(state commands.State) bool {
	return true
}

func (command compactCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
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

func (command compactCommand) String() string {
	return fmt.Sprintf("Compact(%#v)", command)
}

type queryCommand ptarmiganpb.KVQueryRequest

func (command queryCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	res, _ := replicaStore.Query(context.Background(), ptarmiganpb.KVQueryRequest(command))
	return res
}

func (command queryCommand) NextState(state commands.State) commands.State {
	return state
}

func (command queryCommand) PreCondition(state commands.State) bool {
	return true
}

func (command queryCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(state.(*model.ReplicaStoreModel).Query(ptarmiganpb.KVQueryRequest(command)), result)

	if diff != "" {
		fmt.Printf("%s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command queryCommand) String() string {
	return fmt.Sprintf("Query(%#v)", command)
}

type changesCommand struct {
	ptarmiganpb.KVWatchRequest
	Limit int
}

func (command changesCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)

	oldestRevision, err := replicaStore.OldestRevision()

	if err != nil && err != mvcc.ErrNoRevisions {
		panic(err)
	}

	newestRevision, err := replicaStore.NewestRevision()

	if err != nil && err != mvcc.ErrNoRevisions {
		panic(err)
	}

	if command.Start != nil {
		command.Start = &ptarmiganpb.KVWatchCursor{Revision: indexFromRange(command.Start.Revision, oldestRevision, newestRevision), Key: command.Start.Key}
	}

	res, _ := replicaStore.Changes(context.Background(), command.KVWatchRequest, command.Limit)
	return res
}

func (command changesCommand) NextState(state commands.State) commands.State {
	return state
}

func (command changesCommand) PreCondition(state commands.State) bool {
	return true
}

func (command changesCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	oldestRevision := state.(*model.ReplicaStoreModel).OldestRevision()
	newestRevision := state.(*model.ReplicaStoreModel).NewestRevision()

	if command.Start != nil {
		command.Start = &ptarmiganpb.KVWatchCursor{Revision: indexFromRange(command.Start.Revision, oldestRevision, newestRevision), Key: command.Start.Key}
	}

	diff := cmp.Diff(state.(*model.ReplicaStoreModel).Changes(command.KVWatchRequest, command.Limit), result)

	if diff != "" {
		fmt.Printf("%s\n", diff)
		fmt.Printf("expected %#v\n", state.(*model.ReplicaStoreModel).Changes(command.KVWatchRequest, command.Limit))
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command changesCommand) String() string {
	return fmt.Sprintf("Changes(%#v)", command)
}
