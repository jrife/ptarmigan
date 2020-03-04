package state_machine

import (
	"github.com/jrife/ptarmigan/utils/observable_map"
)

type StateMachineID string

// StateMachineMapObserver is an observer callback for an
// ObservableStateMachineMap
type StateMachineMapObserver func(key StateMachineID, value StateMachine)

// ObservableStateMachineMap is a type-safe wrapper for
// ObservableMap that stores state machines
type ObservableStateMachineMap struct {
	observableMap *observable_map.ObservableMap
}

// NewObservableStateMachineMap creates an empty
// ObservableStateMachineMap
func NewObservableStateMachineMap() *ObservableStateMachineMap {
	return &ObservableStateMachineMap{
		observableMap: observable_map.New(),
	}
}

func (stateMachineMap *ObservableStateMachineMap) Put(key StateMachineID, value StateMachine) bool {
	return stateMachineMap.observableMap.Put(key, value)
}

func (stateMachineMap *ObservableStateMachineMap) Delete(key StateMachineID) bool {
	return stateMachineMap.observableMap.Delete(key)
}

func (stateMachineMap *ObservableStateMachineMap) Get(key StateMachineID) (StateMachine, bool) {
	value, ok := stateMachineMap.observableMap.Get(key)

	if !ok {
		return nil, false
	}

	return value.(StateMachine), true
}

func (stateMachineMap *ObservableStateMachineMap) OnAdd(cb StateMachineMapObserver) {
	stateMachineMap.observableMap.OnAdd(func(key interface{}, value interface{}) {
		cb(key.(StateMachineID), value.(StateMachine))
	})
}

func (stateMachineMap *ObservableStateMachineMap) OnUpdate(cb StateMachineMapObserver) {
	stateMachineMap.observableMap.OnUpdate(func(key interface{}, value interface{}) {
		cb(key.(StateMachineID), value.(StateMachine))
	})
}

func (stateMachineMap *ObservableStateMachineMap) OnDelete(cb StateMachineMapObserver) {
	stateMachineMap.observableMap.OnDelete(func(key interface{}, value interface{}) {
		cb(key.(StateMachineID), value.(StateMachine))
	})
}
