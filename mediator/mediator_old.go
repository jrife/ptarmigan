package mediator

// import (
// 	"context"
// 	"fmt"
// 	"io"

// 	etcd_raft "github.com/coreos/etcd/raft"

// 	"github.com/coreos/etcd/raft/raftpb"
// 	"github.com/jrife/ptarmigan/raft"
// 	"github.com/jrife/ptarmigan/replica"
// 	"github.com/jrife/ptarmigan/state_machine"
// 	"github.com/jrife/ptarmigan/state_machine/state_machine_pb"
// 	"github.com/jrife/ptarmigan/storage"
// )

// type Ticker interface {
// 	OnTick(func(replica string))
// 	Quiesce(replica string)
// 	Unquiesce(replica string)
// }

// type Heartbeater interface {
// 	OnHeartbeat(func())
// 	Coalesce()
// 	Uncoalesce()
// 	Quiesce(replica string)
// 	Unquiesce(replica string)
// }

// type SnapshotTransport interface {
// 	StartStateMachineSnapshotDownload(stateMachineID state_machine.StateMachineID, snapshotSpec raftpb.Message)
// 	OnStateMachineSnapshotReceive(func(stateMachineID state_machine.StateMachineID, snapshotSpec raftpb.Message, snapshot io.ReadCloser) error)
// 	OnStateMachineSnapshotRequest(func(stateMachineID state_machine.StateMachineID, snapshotSpec raftpb.Snapshot, snapshotWriter io.Writer) error)
// }

// type CommandHubObserver func(context.Context, state_machine.StateMachineID, state_machine_pb.Command)

// type CommandHub interface {
// 	Submit(ctx context.Context, stateMachineID state_machine.StateMachineID, command state_machine_pb.Command) (interface{}, error)
// 	OnCommand(cb CommandHubObserver)
// 	MaybeRespond(command state_machine_pb.Command, response interface{})
// }

// // Requests interact with Rafts or StateMachine objects
// // Those collections interact with each other through the mediator
// type Mediator struct {
// 	rafts             raft.ObservableRaftMap
// 	raftLocks         LockMap
// 	stateMachines     state_machine.ObservableStateMachineMap
// 	replicas          replica.ObservableReplicaSet
// 	commandHub        CommandHub
// 	ticker            Ticker
// 	heartbeater       Heartbeater
// 	readyQueue        ReadyQueue
// 	snapshotTransport SnapshotTransport
// 	storageProvider   storage.StorageProvider
// }

// func (mediator *Mediator) Init() error {
// 	snapshotWAL, err := mediator.storageProvider.WAL()

// 	if err != nil {
// 		return fmt.Errorf("Could not get snapshot WAL instance from storage provider: %s", err.Error())
// 	}

// 	snapshotWAL.ForEach(func(raftID raft.RaftID, snap raftpb.Snapshot) error {
// 		// If there are any half-finished snapshots they need to be finished up before
// 		// allowing progress for those state machines
// 		return nil
// 	})

// 	mediator.replicas.OnAdd(mediator.handleReplicaAdd)
// 	mediator.replicas.OnDelete(mediator.handleReplicaDelete)
// 	mediator.ticker.OnTick(mediator.handleTick)
// 	mediator.heartbeater.OnHeartbeat(mediator.handleHeartbeat)
// 	mediator.commandHub.OnCommand(mediator.handleCommand)
// 	mediator.snapshotTransport.OnStateMachineSnapshotRequest(mediator.handleStateMachineSnapshotRequest)
// 	mediator.snapshotTransport.OnStateMachineSnapshotReceive(mediator.handleStateMachineSnapshotReceive)

// 	for i := 0; i < 8; i++ {
// 		go mediator.processReadyQueue()
// 	}

// 	return nil
// }

// func (mediator *Mediator) processReadyQueue() {
// 	for {
// 		raftID := mediator.readyQueue.Dequeue()

// 		mediator.processReady(raftID)
// 	}
// }

// func (mediator *Mediator) processReady(raftID raft.RaftID) {
// 	mediator.raftLocks.Lock(raftID)
// 	replica, _ := mediator.replicas.GetByRaft(raftID)

// 	if !replica.Raft().HasReady() {
// 		mediator.raftLocks.Unlock(raftID)

// 		return
// 	}

// 	rd := replica.Raft().Ready()
// 	mediator.raftLocks.Unlock(raftID)

// 	if len(rd.Entries) > 0 {
// 		// TODO Persist entries
// 	}

// 	if !etcd_raft.IsEmptyHardState(rd.HardState) {
// 		// TODO Persist hard state
// 	}

// 	if !etcd_raft.IsEmptySnap(rd.Snapshot) {
// 		// If the state machine is presenting us with a Ready
// 		// that contains a snapshot, surely that indicates that
// 		// we received a snapshot message? If we are intercepting
// 		// received snapshot messages that means we're delaying
// 		// delivery until we have downloaded and stored the full
// 		// snapshot on disk. At this point we should be able to
// 		// safely assume that we have received and stored the
// 		// snapshot.
// 	}

// 	// Send messages
// 	// mediator.raftTransport.Send(rd.Messages)

// 	// Process committed entries
// 	replica.StateMachine().Step(state_machine_pb.Command{})

// 	// Process snapshots

// 	// waits for signal
// 	// processes pending ready's
// 	// sorts messages into buckets based on destination node
// 	// entries persisted
// 	// committedentries consumed by state machines
// 	// snapshots should obtain the snapshot
// }

// func (mediator *Mediator) handleReplicaAdd(replica replica.Replica) {
// 	mediator.raftLocks.Add(replica.Raft().ID())

// 	raftStore, err := mediator.storageProvider.Raft(raft.RaftSpec{ID: (string)(replica.Raft().ID())})

// 	if err != nil {
// 		// TODO handle this somehow
// 	}

// 	raftStore.OnSnapshot(func() (raftpb.Snapshot, error) {
// 		return mediator.takeSnapshot(replica.Raft(), replica.StateMachine())
// 	})
// }

// func (mediator *Mediator) takeSnapshot(raft raft.Raft, stateMachine state_machine.StateMachine) (raftpb.Snapshot, error) {
// 	// 1. This method creates a snapshot of the state machine consistent with the current raft state
// 	// 2. It stages that snapshot in a temporary map and returns an appropriate raftpb.Snapshot instance where the data field can be used
// 	//    to retrieve the staged snapshot.
// 	// Why this is tricky: The recipient can't actually update their durable state until the snapshot message is received.
// 	// Recipient might already have durable state and state machine state. We don't want to overwrite part of it (has to be atomic)
// 	// We could stage a snapshot io stream on the recipient when we receive the snap message. We call Step() with the message then in
// 	// the ready handler when we see a Snapshot in the ready we look for that io stream in our staging area and call ApplySnapshot()
// 	// on the storage (state machine or raft storage?). We need to atomically update statemachine and raft storage
// 	// This implies that the implementation of ApplySnapshot() has to be atomic: If it fails or is canceled there is no effect.
// 	return raftpb.Snapshot{}, nil
// }

// func (mediator *Mediator) handleReplicaDelete(replica replica.Replica) {
// 	mediator.raftLocks.Delete(replica.Raft().ID())
// }

// func (mediator *Mediator) handleRaftTransportReceive(messages []raftpb.Message) {
// 	for _, message := range messages {
// 		if message.Type == raftpb.MsgSnap {
// 			mediator.snapshotTransport.StartStateMachineSnapshotDownload("", message)

// 			continue
// 		}

// 		raft, _ := mediator.rafts.Get("Something")
// 		raft.Step(context.Background(), message)
// 	}
// }

// func (mediator *Mediator) handleStateMachineSnapshotReceive(stateMachineID state_machine.StateMachineID, snapshotSpec raftpb.Message, snapshot io.ReadCloser) error {
// 	stateMachineStore, err := mediator.storageProvider.StateMachine(state_machine.StateMachineSpec{ID: (string)(stateMachineID), StorageClass: "kv"})

// 	if err != nil {
// 		return fmt.Errorf("Could not retrieve state machine store instance for state machine %s from storage provider: %s", stateMachineID, err.Error())
// 	}

// 	err = stateMachineStore.ApplySnapshot(snapshot)

// 	if err != nil {
// 		return err
// 	}

// 	raft, _ := mediator.rafts.Get("")

// 	// We essentially delay delivery of the snapshot message until
// 	// we apply the snapshot to local storage
// 	raft.Step(context.Background(), snapshotSpec)
// 	mediator.readyQueue.Enqueue(raft.ID())

// 	return nil
// }

// func (mediator *Mediator) handleStateMachineSnapshotRequest(stateMachineID state_machine.StateMachineID, snapshotSpec raftpb.Snapshot, snapshotWriter io.Writer) error {
// 	stateMachineStore, err := mediator.storageProvider.StateMachine(state_machine.StateMachineSpec{ID: (string)(stateMachineID), StorageClass: "kv"})

// 	if err != nil {
// 		return fmt.Errorf("Could not retrieve state machine store instance for state machine %s from storage provider: %s", stateMachineID, err.Error())
// 	}

// 	snapshot, err := stateMachineStore.Snapshot()

// 	if err != nil {
// 		return err
// 	}

// 	_, err = io.Copy(snapshotWriter, snapshot)

// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (mediator *Mediator) handleTick(replica string) {
// 	// tick then process ready if necessary
// 	// tick could result in a heartbeat message or new leader election
// 	// being ready
// }

// func (mediator *Mediator) handleHeartbeat() {
// 	// should send queued heartbeats as coalesced messages to their
// 	// respective nodes. Basically mediator sets a rate at which it
// 	// sends out queued heartbeats in an efficient manner
// }

// func (mediator *Mediator) handleCommand(ctx context.Context, stateMachineID state_machine.StateMachineID, command state_machine_pb.Command) {
// 	replica, _ := mediator.replicas.GetByStateMachine(stateMachineID)
// 	marshaledCommand, _ := command.Marshal()

// 	replica.Raft().Propose(context.Background(), marshaledCommand)
// }

// func (mediator *Mediator) handleRaftCommit(replica replica.Replica, data []byte) {
// 	replica.StateMachine().Step(state_machine_pb.Command{})
// 	mediator.commandHub.MaybeRespond(state_machine_pb.Command{}, nil)
// }
