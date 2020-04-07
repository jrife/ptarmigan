package service_host

import (
	"context"
	"io"

	"github.com/jrife/ptarmigan/transport/ptarmiganpb"

	"github.com/coreos/etcd/raft/raftpb"
)

type RaftServiceHost interface {
	AppendEntryToRaftGroup(ctx context.Context, raftGroup string, entry ptarmiganpb.RaftEntry) error
	AddReplicaToRaftGroup(ctx context.Context, raftGroup string, raftReplica string) error
	RemoveReplicaFromRaftGroup(ctx context.Context, raftGroup string, raftReplica string) error
	ReceiveRaftMessages(messages []raftpb.Message)
	ApplySnapshotToRaftGroupReplica(raftReplica string, raftSnap raftpb.Snapshot, stateMachineSnap io.Reader) error
	CreateRaftGroupReplica(raftReplica ptarmiganpb.RaftGroupReplica) error
	DeleteRaftGroupReplica(raftReplica string) error
	ListRaftGroupReplicas() ([]ptarmiganpb.RaftGroupReplica, error)
}
