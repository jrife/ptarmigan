package controllers

import "context"

type RaftGroups interface {
}

type StorageHosts interface {
}

type RaftGroupController struct {
	raftGroups   RaftGroups
	storageHosts StorageHosts
}

func (controller *RaftGroupController) Run(ctx context.Context, fencingToken int64) {
	// Determines the placement of raft group replicas
	// Handles raft group replica lifecycle (provisioning, decomissioning, rebalancing)
	// Needs to respond to storage host additions and removals to coordinate raft group replica movement
	// while maintaining availability
}
