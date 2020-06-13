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
			return fmt.Sprintf("kvs don't match at revision %d: %s", revision, diff), nil
		}
	}

	changes, err := replicaStore.Changes(context.Background(), ptarmiganpb.KVWatchRequest{}, -1)

	if err != nil {
		return "", fmt.Errorf("could not retrieve changes from replica store: %s", err)
	}

	if diff := cmp.Diff(model.Changes(ptarmiganpb.KVWatchRequest{}, -1), changes); diff != "" {
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
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command txnCommand) String() string {
	str := "Txn("

	if command.Compare != nil {
		str += "\n  Compare(\n"

		for i, compare := range command.Compare {
			str += fmt.Sprintf("    %d: %s\n", i, compare.String())
		}

		str += "  )\n"
	}

	if command.Success != nil {
		str += "\n  Success(\n"

		for i, op := range command.Success {
			str += fmt.Sprintf("    %d: %s\n", i, op.String())
		}

		str += "  )\n"
	}

	if command.Failure != nil {
		str += "\n  Failure(\n"

		for i, op := range command.Failure {
			str += fmt.Sprintf("    %d: %s\n", i, op.String())
		}

		str += "  )\n"
	}

	str += ")"

	return str
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
		fmt.Printf("Err: %#v\n", err)

		return &gopter.PropResult{Status: gopter.PropFalse, Error: err}
	}

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
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
		fmt.Printf("Diff: %s\n", diff)
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
		fmt.Printf("Diff: %s\n", diff)
		fmt.Printf("expected %#v\n", state.(*model.ReplicaStoreModel).Changes(command.KVWatchRequest, command.Limit))
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command changesCommand) String() string {
	return fmt.Sprintf("Changes(%#v)", command)
}

type createLeaseCommand struct {
	TTL int64
}

func (command createLeaseCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	index, err := replicaStore.Index(context.Background())

	if err != nil {
		panic(err)
	}

	res, _ := replicaStore.Apply(index+1).CreateLease(context.Background(), command.TTL)
	return res
}

func (command createLeaseCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyCreateLease(state.(*model.ReplicaStoreModel).Index()+1, command.TTL)
	return state
}

func (command createLeaseCommand) PreCondition(state commands.State) bool {
	return true
}

func (command createLeaseCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	resp := state.(*model.ReplicaStoreModel).LastResponse().(ptarmiganpb.Lease)
	diff := cmp.Diff(resp, result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command createLeaseCommand) String() string {
	return fmt.Sprintf("CreateLease(%#v)", command.TTL)
}

type revokeLeaseCommand struct {
	ID int64
}

func (command revokeLeaseCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	index, err := replicaStore.Index(context.Background())

	if err != nil {
		panic(err)
	}

	replicaStore.Apply(index+1).RevokeLease(context.Background(), command.ID)
	return sut
}

func (command revokeLeaseCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyRevokeLease(state.(*model.ReplicaStoreModel).Index()+1, command.ID)
	return state
}

func (command revokeLeaseCommand) PreCondition(state commands.State) bool {
	return true
}

func (command revokeLeaseCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	model := state.(*model.ReplicaStoreModel)
	replicaStore := result.(storage.ReplicaStore)

	diff, err := replicaStoreModelDiff(replicaStore, model)

	if err != nil {
		fmt.Printf("Err: %#v\n", err)

		return &gopter.PropResult{Status: gopter.PropFalse, Error: err}
	}

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command revokeLeaseCommand) String() string {
	return fmt.Sprintf("RevokeLease(%#v)", command.ID)
}

type leasesCommand struct {
}

func (command leasesCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	res, _ := replicaStore.Leases(context.Background())
	return res
}

func (command leasesCommand) NextState(state commands.State) commands.State {
	return state
}

func (command leasesCommand) PreCondition(state commands.State) bool {
	return true
}

func (command leasesCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(state.(*model.ReplicaStoreModel).Leases(), result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command leasesCommand) String() string {
	return fmt.Sprintf("Leases()")
}

type getLeaseCommand struct {
	ID int64
}

func (command getLeaseCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	res, _ := replicaStore.GetLease(context.Background(), command.ID)
	return res
}

func (command getLeaseCommand) NextState(state commands.State) commands.State {
	return state
}

func (command getLeaseCommand) PreCondition(state commands.State) bool {
	return true
}

func (command getLeaseCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(state.(*model.ReplicaStoreModel).GetLease(command.ID), result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command getLeaseCommand) String() string {
	return fmt.Sprintf("GetLease(%#v)", command.ID)
}

type newestRevisionCommand struct {
}

func (command newestRevisionCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	res, _ := replicaStore.NewestRevision()
	return res
}

func (command newestRevisionCommand) NextState(state commands.State) commands.State {
	return state
}

func (command newestRevisionCommand) PreCondition(state commands.State) bool {
	return true
}

func (command newestRevisionCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(state.(*model.ReplicaStoreModel).NewestRevision(), result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command newestRevisionCommand) String() string {
	return fmt.Sprintf("NewestRevision()")
}

type oldestRevisionCommand struct {
}

func (command oldestRevisionCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	res, _ := replicaStore.OldestRevision()
	return res
}

func (command oldestRevisionCommand) NextState(state commands.State) commands.State {
	return state
}

func (command oldestRevisionCommand) PreCondition(state commands.State) bool {
	return true
}

func (command oldestRevisionCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(state.(*model.ReplicaStoreModel).OldestRevision(), result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command oldestRevisionCommand) String() string {
	return fmt.Sprintf("OldestRevision()")
}
