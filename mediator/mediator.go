package mediator

import (
	"context"

	etcd_raft "github.com/coreos/etcd/raft"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/raft"
	"github.com/jrife/ptarmigan/replica"
	"github.com/jrife/ptarmigan/state_machine"
	"github.com/jrife/ptarmigan/state_machine/state_machine_pb"
)

type Ticker interface {
	OnTick(func(replica string))
	Quiesce(replica string)
	Unquiesce(replica string)
}

type Heartbeater interface {
	OnHeartbeat(func())
	Coalesce()
	Uncoalesce()
	Quiesce(replica string)
	Unquiesce(replica string)
}

type SnapshotDownloader interface {
	DownloadSnapshot(snapshot raftpb.Snapshot) error
}

type CommandHubObserver func(context.Context, state_machine.StateMachineID, state_machine_pb.Command)

type CommandHub interface {
	Submit(ctx context.Context, stateMachineID state_machine.StateMachineID, command state_machine_pb.Command) (interface{}, error)
	OnCommand(cb CommandHubObserver)
	MaybeRespond(command state_machine_pb.Command, response interface{})
}

// Requests interact with Rafts or StateMachine objects
// Those collections interact with each other through the mediator
type Mediator struct {
	rafts              raft.ObservableRaftMap
	raftLocks          LockMap
	stateMachines      state_machine.ObservableStateMachineMap
	replicas           replica.ObservableReplicaSet
	commandHub         CommandHub
	raftTransport      raft.RaftTransport
	ticker             Ticker
	heartbeater        Heartbeater
	readyQueue         ReadyQueue
	snapshotDownloader SnapshotDownloader
	raftStorage        etcd_raft.Storage
}

func (mediator *Mediator) Init() {
	mediator.replicas.OnAdd(mediator.handleReplicaAdd)
	mediator.replicas.OnDelete(mediator.handleReplicaDelete)
	mediator.raftTransport.OnReceive(mediator.handleRaftTransportReceive)
	mediator.ticker.OnTick(mediator.handleTick)
	mediator.heartbeater.OnHeartbeat(mediator.handleHeartbeat)
	mediator.commandHub.OnCommand(mediator.handleCommand)

	for i := 0; i < 8; i++ {
		go mediator.processReadyQueue()
	}
}

func (mediator *Mediator) processReadyQueue() {
	for {
		raftID := mediator.readyQueue.Dequeue()

		mediator.processReady(raftID)
	}
}

func (mediator *Mediator) processReady(raftID raft.RaftID) {
	mediator.raftLocks.Lock(raftID)
	replica, _ := mediator.replicas.GetByRaft(raftID)

	if !replica.Raft().HasReady() {
		mediator.raftLocks.Unlock(raftID)

		return
	}

	rd := replica.Raft().Ready()
	mediator.raftLocks.Unlock(raftID)

	if len(rd.Entries) > 0 {
		// TODO Persist entries
	}

	if !etcd_raft.IsEmptyHardState(rd.HardState) {
		// TODO Persist hard state
	}

	if !etcd_raft.IsEmptySnap(rd.Snapshot) {
		// If the state machine is presenting us with a Ready
		// that contains a snapshot, surely that indicates that
		// we received a snapshot message? If we are intercepting
		// received snapshot messages that means we're delaying
		// delivery until we have downloaded and stored the full
		// snapshot on disk. At this point we should be able to
		// safely assume that we have received and stored the
		// snapshot.
	}

	// Send messages
	mediator.raftTransport.Send(rd.Messages)

	// Process committed entries
	replica.StateMachine().Step(state_machine_pb.Command{})

	// Process snapshots

	// waits for signal
	// processes pending ready's
	// sorts messages into buckets based on destination node
	// entries persisted
	// committedentries consumed by state machines
	// snapshots should obtain the snapshot
}

func (mediator *Mediator) handleReplicaAdd(replica replica.Replica) {
	mediator.raftLocks.Add(replica.Raft().ID())
}

func (mediator *Mediator) handleReplicaDelete(replica replica.Replica) {
	mediator.raftLocks.Delete(replica.Raft().ID())
}

func (mediator *Mediator) handleRaftTransportReceive(messages []raftpb.Message) {
	for _, message := range messages {
		if message.Type == raftpb.MsgSnap {
			mediator.snapshotDownloader.DownloadSnapshot(message.Snapshot)

			continue
		}

		raft, _ := mediator.rafts.Get("Something")
		raft.Step(context.Background(), message)
	}
}

func (mediator *Mediator) handleTick(replica string) {
	// tick then process ready if necessary
	// tick could result in a heartbeat message or new leader election
	// being ready
}

func (mediator *Mediator) handleHeartbeat() {
	// should send queued heartbeats as coalesced messages to their
	// respective nodes. Basically mediator sets a rate at which it
	// sends out queued heartbeats in an efficient manner
}

func (mediator *Mediator) handleCommand(ctx context.Context, stateMachineID state_machine.StateMachineID, command state_machine_pb.Command) {
	replica, _ := mediator.replicas.GetByStateMachine(stateMachineID)
	marshaledCommand, _ := command.Marshal()

	replica.Raft().Propose(context.Background(), marshaledCommand)
}

func (mediator *Mediator) handleRaftCommit(replica replica.Replica, data []byte) {
	replica.StateMachine().Step(state_machine_pb.Command{})
	mediator.commandHub.MaybeRespond(state_machine_pb.Command{}, nil)
}
