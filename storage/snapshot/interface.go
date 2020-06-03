package snapshot

import (
	"context"
	"io"
)

// Acceptor describes something that can
// apply a snapshot
type Acceptor interface {
	ApplySnapshot(ctx context.Context, snap io.Reader) error
}

// Source describes something that can
// generate a snapshot
type Source interface {
	Snapshot(ctx context.Context) (io.ReadCloser, error)
}
