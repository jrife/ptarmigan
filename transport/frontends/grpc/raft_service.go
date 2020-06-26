package grpc

import (
	"context"
	"fmt"
	"io"

	"github.com/jrife/ptarmigan/transport/service_host"

	"github.com/jrife/ptarmigan/transport/frontends/grpc/pb"
	"github.com/jrife/ptarmigan/transport/ptarmiganpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ pb.RaftServer = (*RaftServer)(nil)

// RaftServer implements the gRPC
// servers Raft service. It mostly
// forwards requests on to the
// ptarmigan server.
type RaftServer struct {
	host service_host.RaftServiceHost
}

func (raftServer *RaftServer) ApplySnapshot(snapshot pb.Raft_ApplySnapshotServer) error {
	metadata, err := snapshot.Recv()

	if err != nil {
		return err
	}

	if metadata.GetMetadata() == nil {
		return status.Newf(codes.InvalidArgument, "First chunk is not a raftpb.Snapshot instance").Err()
	}

	if err := raftServer.host.ApplySnapshot(*metadata.GetMetadata(), &chunkStreamReader{chunkStream: snapshot}); err != nil {
		return status.Newf(codes.Unknown, "Unable to apply snapshot: %s", err.Error()).Err()
	}

	return nil
}

func (raftServer *RaftServer) SendMessages(ctx context.Context, messages *pb.RaftMessages) (*pb.Empty, error) {
	raftServer.host.Receive(messages.Messages)

	return &pb.Empty{}, nil
}

type chunkStream interface {
	Recv() (*ptarmiganpb.Chunk, error)
}

var _ io.Reader = (*chunkStreamReader)(nil)

type chunkStreamReader struct {
	chunkStream chunkStream
	chunk       []byte
	err         error
}

func (reader *chunkStreamReader) nextChunk() error {
	if reader.err != nil {
		return reader.err
	}

	if len(reader.chunk) == 0 {
		chunk, err := reader.chunkStream.Recv()

		if err != nil {
			reader.err = err
		} else if chunk.GetMetadata() != nil {
			reader.err = fmt.Errorf("Chunk was snapshot metadata, not data")
		} else {
			reader.chunk = chunk.GetData()
		}
	}

	return reader.err
}

func (reader *chunkStreamReader) Read(p []byte) (n int, err error) {
	for n != len(p) {
		if err := reader.nextChunk(); err != nil {
			break
		}

		c := copy(p[n:], reader.chunk)
		reader.chunk = reader.chunk[c:]
		n += c
	}

	return 0, reader.err
}
