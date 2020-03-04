package mediator

import (
	"sync"

	"github.com/jrife/ptarmigan/raft"
)

type ReadyQueue interface {
	Enqueue(raftID raft.RaftID)
	Dequeue() raft.RaftID
}

type ReadyQueueImpl struct {
	mu    sync.Mutex
	ready map[raft.RaftID]bool
	queue chan raft.RaftID
}

// This is a wrapper around a buffered channel
// that ensures the same ID isn't queued more than
// once
func NewReadyQueue(capacity int) *ReadyQueueImpl {
	return &ReadyQueueImpl{
		ready: make(map[raft.RaftID]bool),
		queue: make(chan raft.RaftID, capacity),
	}
}

func (readyQueue *ReadyQueueImpl) Enqueue(raftID raft.RaftID) {
	readyQueue.mu.Lock()

	if _, ok := readyQueue.ready[raftID]; ok {
		readyQueue.mu.Unlock()

		return
	}

	readyQueue.ready[raftID] = true
	readyQueue.mu.Unlock()

	readyQueue.queue <- raftID
}

func (raftQueue *ReadyQueueImpl) Dequeue() raft.RaftID {
	raftID := <-raftQueue.queue

	raftQueue.mu.Lock()
	delete(raftQueue.ready, raftID)
	raftQueue.mu.Unlock()

	return raftID
}
