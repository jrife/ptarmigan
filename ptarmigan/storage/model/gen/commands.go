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

type txnCommand struct {
	txnRequest ptarmiganpb.KVTxnRequest
	index      uint64
}

func (command txnCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	res, _ := replicaStore.Apply(command.index).Txn(context.Background(), command.txnRequest)
	return res
}

func (command txnCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyTxn(command.index, command.txnRequest)
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
	revision int64
	index    uint64
}

func (command compactCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	replicaStore.Apply(command.index).Compact(context.Background(), command.revision)
	return sut
}

func (command compactCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyCompact(command.index, command.revision)
	return state
}

func (command compactCommand) PreCondition(state commands.State) bool {
	return true
}

func (command compactCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	model := state.(*model.ReplicaStoreModel)
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
	watchRequest ptarmiganpb.KVWatchRequest
	limit        int
}

func (command changesCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
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
	diff := cmp.Diff(state.(*model.ReplicaStoreModel).Changes(command.watchRequest, command.limit), result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		fmt.Printf("expected %#v\n", state.(*model.ReplicaStoreModel).Changes(command.watchRequest, command.limit))
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command changesCommand) String() string {
	return fmt.Sprintf("Changes(%#v)", command)
}

type createLeaseCommand struct {
	ttl   int64
	index uint64
}

func (command createLeaseCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	res, _ := replicaStore.Apply(command.index).CreateLease(context.Background(), command.ttl)
	return res
}

func (command createLeaseCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyCreateLease(command.index, command.ttl)
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
	return fmt.Sprintf("CreateLease(%#v)", command.ttl)
}

type revokeLeaseCommand struct {
	id    int64
	index uint64
}

func (command revokeLeaseCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	replicaStore.Apply(command.index).RevokeLease(context.Background(), command.id)
	return sut
}

func (command revokeLeaseCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).ApplyRevokeLease(command.index, command.id)
	return state
}

func (command revokeLeaseCommand) PreCondition(state commands.State) bool {
	return true
}

func (command revokeLeaseCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	model := state.(*model.ReplicaStoreModel)
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

type indexCommand struct {
}

func (command indexCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
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
	diff := cmp.Diff(state.(*model.ReplicaStoreModel).Index(), result)

	if diff != "" {
		fmt.Printf("Diff: %s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command indexCommand) String() string {
	return fmt.Sprintf("Index()")
}

type multiCommand struct {
	i       int
	command commands.Command
}

func (command multiCommand) Run(sut commands.SystemUnderTest) commands.Result {
	return command.command.Run(sut.([]storage.ReplicaStore)[command.i])
}

func (command multiCommand) NextState(state commands.State) commands.State {
	return command.command.NextState(state.([]*model.ReplicaStoreModel)[command.i])
}

func (command multiCommand) PreCondition(state commands.State) bool {
	return command.command.PreCondition(state.([]*model.ReplicaStoreModel)[command.i])
}

func (command multiCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	return command.command.PostCondition(state, result)
}

func (command multiCommand) String() string {
	return fmt.Sprintf("%d: %s", command.i, command.command.String())
}
