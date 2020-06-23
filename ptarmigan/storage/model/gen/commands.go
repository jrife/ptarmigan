package gen

import (
	"context"
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
	"github.com/jrife/flock/ptarmigan/storage/model"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
)

type command struct {
	replicaStore int
}

func (command command) sut(sut commands.SystemUnderTest) storage.ReplicaStore {
	if s, ok := sut.(ReplicaStoreSet); ok {
		return s.Get(command.replicaStore)
	}

	return sut.(storage.ReplicaStore)
}

func (command command) state(state commands.State) *model.ReplicaStoreModel {
	if s, ok := state.([]model.ReplicaStoreModel); ok {
		return &s[command.replicaStore]
	}

	return state.(*model.ReplicaStoreModel)
}

type txnCommand struct {
	command
	txnRequest ptarmiganpb.KVTxnRequest
	index      uint64
}

func (command txnCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	res, _ := replicaStore.Apply(command.index).Txn(context.Background(), command.txnRequest)
	return res
}

func (command txnCommand) NextState(state commands.State) commands.State {
	command.state(state).ApplyTxn(command.index, command.txnRequest)
	return state
}

func (command txnCommand) PreCondition(state commands.State) bool {
	return true
}

func (command txnCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	resp := command.state(state).LastResponse().(ptarmiganpb.KVTxnResponse)
	diff := cmp.Diff(resp, result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command txnCommand) String() string {
	str := "Txn("

	if command.txnRequest.Compare != nil {
		str += "\n  Compare(\n"

		for i, compare := range command.txnRequest.Compare {
			str += fmt.Sprintf("    %d: %s\n", i, compare.String())
		}

		str += "  )\n"
	}

	if command.txnRequest.Success != nil {
		str += "\n  Success(\n"

		for i, op := range command.txnRequest.Success {
			str += fmt.Sprintf("    %d: %s\n", i, op.String())
		}

		str += "  )\n"
	}

	if command.txnRequest.Failure != nil {
		str += "\n  Failure(\n"

		for i, op := range command.txnRequest.Failure {
			str += fmt.Sprintf("    %d: %s\n", i, op.String())
		}

		str += "  )\n"
	}

	str += ")"

	return str
}

type compactCommand struct {
	command
	revision int64
	index    uint64
}

func (command compactCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	replicaStore.Apply(command.index).Compact(context.Background(), command.revision)
	return replicaStore
}

func (command compactCommand) NextState(state commands.State) commands.State {
	command.state(state).ApplyCompact(command.index, command.revision)
	return state
}

func (command compactCommand) PreCondition(state commands.State) bool {
	return true
}

func (command compactCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	model := command.state(state)
	replicaStore := result.(storage.ReplicaStore)

	diff, err := ReplicaStoreModelDiff(replicaStore, model)

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

type queryCommand struct {
	command
	queryRequest ptarmiganpb.KVQueryRequest
}

func (command queryCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	res, _ := replicaStore.Query(context.Background(), ptarmiganpb.KVQueryRequest(command.queryRequest))
	return res
}

func (command queryCommand) NextState(state commands.State) commands.State {
	return state
}

func (command queryCommand) PreCondition(state commands.State) bool {
	return true
}

func (command queryCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(command.state(state).Query(ptarmiganpb.KVQueryRequest(command.queryRequest)), result)

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
	command
	watchRequest ptarmiganpb.KVWatchRequest
	limit        int
}

func (command changesCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	res, _ := replicaStore.Changes(context.Background(), command.watchRequest, command.limit)
	return res
}

func (command changesCommand) NextState(state commands.State) commands.State {
	return state
}

func (command changesCommand) PreCondition(state commands.State) bool {
	return true
}

func (command changesCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(command.state(state).Changes(command.watchRequest, command.limit), result)

	if diff != "" {
		fmt.Printf("Diff (%#v): %s\n", command.watchRequest, diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command changesCommand) String() string {
	return fmt.Sprintf("Changes(%#v)", command)
}

type createLeaseCommand struct {
	command
	ttl   int64
	index uint64
}

func (command createLeaseCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	res, _ := replicaStore.Apply(command.index).CreateLease(context.Background(), command.ttl)
	return res
}

func (command createLeaseCommand) NextState(state commands.State) commands.State {
	command.state(state).ApplyCreateLease(command.index, command.ttl)
	return state
}

func (command createLeaseCommand) PreCondition(state commands.State) bool {
	return true
}

func (command createLeaseCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	resp := command.state(state).LastResponse().(ptarmiganpb.Lease)
	diff := cmp.Diff(resp, result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command createLeaseCommand) String() string {
	return fmt.Sprintf("CreateLease(%#v)", command.ttl)
}

type revokeLeaseCommand struct {
	command
	id    int64
	index uint64
}

func (command revokeLeaseCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	replicaStore.Apply(command.index).RevokeLease(context.Background(), command.id)
	return replicaStore
}

func (command revokeLeaseCommand) NextState(state commands.State) commands.State {
	command.state(state).ApplyRevokeLease(command.index, command.id)
	return state
}

func (command revokeLeaseCommand) PreCondition(state commands.State) bool {
	return true
}

func (command revokeLeaseCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	model := command.state(state)
	replicaStore := result.(storage.ReplicaStore)

	diff, err := ReplicaStoreModelDiff(replicaStore, model)

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
	return fmt.Sprintf("RevokeLease(%#v)", command.id)
}

type leasesCommand struct {
	command
}

func (command leasesCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
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
	diff := cmp.Diff(command.state(state).Leases(), result)

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
	command
	id int64
}

func (command getLeaseCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	res, _ := replicaStore.GetLease(context.Background(), command.id)
	return res
}

func (command getLeaseCommand) NextState(state commands.State) commands.State {
	return state
}

func (command getLeaseCommand) PreCondition(state commands.State) bool {
	return true
}

func (command getLeaseCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(command.state(state).GetLease(command.id), result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command getLeaseCommand) String() string {
	return fmt.Sprintf("GetLease(%#v)", command.id)
}

type newestRevisionCommand struct {
	command
}

func (command newestRevisionCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	res, _ := replicaStore.NewestRevision(context.Background())
	return res
}

func (command newestRevisionCommand) NextState(state commands.State) commands.State {
	return state
}

func (command newestRevisionCommand) PreCondition(state commands.State) bool {
	return true
}

func (command newestRevisionCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(command.state(state).NewestRevision(), result)

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
	command
}

func (command oldestRevisionCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	res, _ := replicaStore.OldestRevision(context.Background())
	return res
}

func (command oldestRevisionCommand) NextState(state commands.State) commands.State {
	return state
}

func (command oldestRevisionCommand) PreCondition(state commands.State) bool {
	return true
}

func (command oldestRevisionCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(command.state(state).OldestRevision(), result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command oldestRevisionCommand) String() string {
	return fmt.Sprintf("OldestRevision()")
}

type indexCommand struct {
	command
}

func (command indexCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := command.sut(sut)
	res, _ := replicaStore.Index(context.Background())
	return res
}

func (command indexCommand) NextState(state commands.State) commands.State {
	return state
}

func (command indexCommand) PreCondition(state commands.State) bool {
	return true
}

func (command indexCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diff := cmp.Diff(command.state(state).Index(), result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command indexCommand) String() string {
	return fmt.Sprintf("Index()")
}

type snapshotCommand struct {
	source int
	dest   int
}

func (command snapshotCommand) Run(sut commands.SystemUnderTest) commands.Result {
	source := sut.(ReplicaStoreSet).Get(command.source)
	dest := sut.(ReplicaStoreSet).Get(command.dest)
	snap, err := source.Snapshot(context.Background())

	if err != nil {
		panic(err)
	}

	if err := dest.ApplySnapshot(context.Background(), snap); err != nil {
		panic(err)
	}

	return sut
}

func (command snapshotCommand) NextState(state commands.State) commands.State {
	source := state.([]model.ReplicaStoreModel)[command.source]

	state.([]model.ReplicaStoreModel)[command.dest] = *source.DeepCopy()

	return state
}

func (command snapshotCommand) PreCondition(state commands.State) bool {
	return true
}

func (command snapshotCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	sourceModel := state.([]model.ReplicaStoreModel)[command.source]
	sourceSut := result.(ReplicaStoreSet).Get(command.source)
	destSut := result.(ReplicaStoreSet).Get(command.source)

	diff, err := ReplicaStoreModelDiff(sourceSut, &sourceModel)

	if err != nil {
		fmt.Printf("Err: %#v\n", err)

		return &gopter.PropResult{Status: gopter.PropFalse, Error: err}
	}

	if diff != "" {
		fmt.Printf("Diff (source model vs source sut): %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	diff, err = ReplicaStoreModelDiff(destSut, &sourceModel)

	if err != nil {
		fmt.Printf("Err: %#v\n", err)

		return &gopter.PropResult{Status: gopter.PropFalse, Error: err}
	}

	if diff != "" {
		fmt.Printf("Diff (source model vs dest sut): %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command snapshotCommand) String() string {
	return fmt.Sprintf("Snapshot(%d -> %d)", command.source, command.dest)
}
