package mediator

import (
	"io"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/stateful_services"
	"github.com/jrife/ptarmigan/transport/service_host"
)

var _ service_host.PtarmiganHost = (*Mediator)(nil)

type Mediator struct {
}

func (mediator *Mediator) Version() string {
	return ""
}

func (mediator *Mediator) Receive(messages []raftpb.Message) {
}

func (mediator *Mediator) ApplySnapshot(raftSnap raftpb.Snapshot, stateMachineSnap io.Reader) error {
	return nil
}

func (mediator *Mediator) StatefulServiceProviders() []stateful_services.StatefulServiceProvider {
	return nil
}
