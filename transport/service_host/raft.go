package service_host

import (
	"io"

	"github.com/coreos/etcd/raft/raftpb"
)

type RaftServiceHost interface {
	Receive(messages []raftpb.Message)
	ApplySnapshot(raftSnap raftpb.Snapshot, stateMachineSnap io.Reader) error
}
