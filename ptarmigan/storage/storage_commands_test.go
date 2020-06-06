package storage_test

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

type QueryCommand ptarmiganpb.KVQueryRequest

func (command QueryCommand) Run(sut commands.SystemUnderTest) commands.Result {
	replicaStore := sut.(storage.ReplicaStore)
	res, _ := replicaStore.Query(context.Background(), ptarmiganpb.KVQueryRequest(command))
	return res
}

func (command QueryCommand) NextState(state commands.State) commands.State {
	state.(*model.ReplicaStoreModel).Query(ptarmiganpb.KVQueryRequest(command))
	return state
}

func (command QueryCommand) PreCondition(state commands.State) bool {
	return true
}

func (command QueryCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	resp := state.(*model.ReplicaStoreModel).LastResponse().(ptarmiganpb.KVQueryResponse)
	diff := cmp.Diff(resp, result)

	if diff != "" {
		fmt.Printf("%s\n", diff)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}

	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (command QueryCommand) String() string {
	return fmt.Sprintf("Query(%#v)", command)
}
