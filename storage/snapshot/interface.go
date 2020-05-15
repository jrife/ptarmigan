package snapshot

import (
	"io"
)

// Acceptor describes something that can
// apply a snapshot
type Acceptor interface {
	ApplySnapshot(snap io.Reader) error
}

// Source describes something that can
// generate a snapshot
type Source interface {
	Snapshot() (io.ReadCloser, error)
}
