package clients

import (
	"io"

	"github.com/coreos/etcd/raft/raftpb"
)

type RaftServiceClient interface {
	SendRaftMessages(messages []raftpb.Message) error
	ApplySnapshot(raftSnap raftpb.Snapshot, stateMachineSnap io.Reader) error
}
