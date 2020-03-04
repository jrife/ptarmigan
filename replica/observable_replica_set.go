package replica

import (
	"sync"

	"github.com/jrife/ptarmigan/raft"
	"github.com/jrife/ptarmigan/state_machine"
	"github.com/jrife/ptarmigan/utils/observable_map"
)

// ReplicaSetObserver is an observer callback for an
// ObservableReplicaSet
type ReplicaSetObserver func(replica Replica)

// ObservableReplicaSet is a type-safe wrapper for
// ObservableMap that stores replicas.
type ObservableReplicaSet struct {
	mu               sync.Mutex
	byRaftID         *observable_map.ObservableMap
	byStateMachineID *observable_map.ObservableMap
}

// NewObservableStateMachineMap creates an empty
// ObservableStateMachineMap
func NewObservableReplicaSet() *ObservableReplicaSet {
	return &ObservableReplicaSet{
		byRaftID:         observable_map.New(),
		byStateMachineID: observable_map.New(),
	}
}

func (observableReplicaSet *ObservableReplicaSet) ensureInvariants(replica Replica) {
	if replica == nil {
		panic("nil replica")
	}

	if replica.Raft() == nil {
		panic("nil Raft")
	}

	if replica.StateMachine() == nil {
		panic("nil StateMachine")
	}
}

// Add adds a replica to the set. It maintains the
// 1-1 invariant for rafts and state machines. If
// a caller tries to add two replicas with the same
// state machine or raft Add will panic. If a caller
// calls Add with a nil replica it will panic. If
// a caller calls Add with a replica whose state
// machine or raft is nil it will panic.
func (observableReplicaSet *ObservableReplicaSet) Add(replica Replica) {
	observableReplicaSet.ensureInvariants(replica)

	observableReplicaSet.mu.Lock()
	defer observableReplicaSet.mu.Unlock()

	// Add is idempotent. The same replica can be added more
	// than once with no effect.
	_, raftIDExists := observableReplicaSet.byRaftID.Get(replica.Raft().ID())
	_, smIDExists := observableReplicaSet.byStateMachineID.Get(replica.StateMachine().ID())

	// XOR
	if raftIDExists && !smIDExists || !raftIDExists && smIDExists {
		panic("1-1 invariant violation")
	} else if !raftIDExists && !smIDExists {
		observableReplicaSet.byRaftID.Put(replica.Raft().ID(), replica)
		observableReplicaSet.byStateMachineID.Put(replica.StateMachine().ID(), replica)
	}
}

func (observableReplicaSet *ObservableReplicaSet) Delete(replica Replica) {
	observableReplicaSet.ensureInvariants(replica)

	observableReplicaSet.mu.Lock()
	defer observableReplicaSet.mu.Unlock()

	observableReplicaSet.byRaftID.Delete(replica.Raft().ID())
	observableReplicaSet.byStateMachineID.Delete(replica.StateMachine().ID())
}

func (observableReplicaSet *ObservableReplicaSet) GetByStateMachine(key state_machine.StateMachineID) (Replica, bool) {
	value, ok := observableReplicaSet.byStateMachineID.Get(key)

	if !ok {
		return nil, false
	}

	return value.(Replica), true
}

func (observableReplicaSet *ObservableReplicaSet) GetByRaft(key raft.RaftID) (Replica, bool) {
	value, ok := observableReplicaSet.byRaftID.Get(key)

	if !ok {
		return nil, false
	}

	return value.(Replica), true
}

func (observableReplicaSet *ObservableReplicaSet) OnAdd(cb ReplicaSetObserver) {
	observableReplicaSet.byRaftID.OnAdd(func(key interface{}, value interface{}) {
		cb(value.(Replica))
	})
}

func (observableReplicaSet *ObservableReplicaSet) OnDelete(cb ReplicaSetObserver) {
	observableReplicaSet.byRaftID.OnDelete(func(key interface{}, value interface{}) {
		cb(value.(Replica))
	})
}
