package raft

import (
	"github.com/jrife/ptarmigan/utils/observable_map"
)

type RaftID string

// RaftMapObserver is an observer callback for an
// ObservableRaftMap
type RaftMapObserver func(key RaftID, value Raft)

// ObservableRaftMap is a type-safe wrapper for
// ObservableMap that stores raft state machines
type ObservableRaftMap struct {
	observableMap *observable_map.ObservableMap
}

// NewObservableRaftMap creates an empty
// ObservableRaftMap
func NewObservableRaftMap() *ObservableRaftMap {
	return &ObservableRaftMap{
		observableMap: observable_map.New(),
	}
}

func (raftMap *ObservableRaftMap) Put(key RaftID, value Raft) bool {
	return raftMap.observableMap.Put(key, value)
}

func (raftMap *ObservableRaftMap) Delete(key RaftID) bool {
	return raftMap.observableMap.Delete(key)
}

func (raftMap *ObservableRaftMap) Get(key RaftID) (Raft, bool) {
	value, ok := raftMap.observableMap.Get(key)

	if !ok {
		return nil, false
	}

	return value.(Raft), true
}

func (raftMap *ObservableRaftMap) OnAdd(cb RaftMapObserver) {
	raftMap.observableMap.OnAdd(func(key interface{}, value interface{}) {
		cb(key.(RaftID), value.(Raft))
	})
}

func (raftMap *ObservableRaftMap) OnUpdate(cb RaftMapObserver) {
	raftMap.observableMap.OnUpdate(func(key interface{}, value interface{}) {
		cb(key.(RaftID), value.(Raft))
	})
}

func (raftMap *ObservableRaftMap) OnDelete(cb RaftMapObserver) {
	raftMap.observableMap.OnDelete(func(key interface{}, value interface{}) {
		cb(key.(RaftID), value.(Raft))
	})
}
