package services

import (
	"io"

	"github.com/coreos/etcd/raft/raftpb"
)

type RaftClient interface {
	SendRaftMessages(messages []raftpb.Message)
}

type RaftService interface {
	Receive(messages []raftpb.Message)
	ApplySnapshot(raftSnap raftpb.Snapshot, stateMachineSnap io.Reader) error
}
