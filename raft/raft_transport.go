package raft

import (
	"github.com/coreos/etcd/raft/raftpb"
)

type RaftTransport interface {
	Receive(messages []raftpb.Message)
	Send(messages []raftpb.Message)
	OnReceive(func(messages []raftpb.Message))
}
