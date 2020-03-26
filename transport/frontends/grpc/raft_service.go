package grpc

import (
	"context"
	"io"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/jrife/ptarmigan/transport/frontends/grpc/pb"
	"github.com/jrife/ptarmigan/transport/ptarmiganpb"
	"github.com/jrife/ptarmigan/transport/services"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ pb.RaftServer = (*RaftServer)(nil)

// RaftServer implements the gRPC
// servers Raft service. It mostly
// forwards requests on to the
// ptarmigan server.
type RaftServer struct {
	raftService services.RaftService
}

func (raftServer *RaftServer) ApplySnapshot(snapshot pb.Raft_ApplySnapshotServer) error {
	// TODO first chunk should actually be metadata
	if err := raftServer.raftService.ApplySnapshot(raftpb.Snapshot{}, &chunkStreamReader{chunkStream: snapshot}); err != nil {
		return status.Newf(codes.Internal, "Unable to apply snapshot: %s", err.Error()).Err()
	}

	return nil
}

func (raftServer *RaftServer) SendMessages(ctx context.Context, messages *pb.RaftMessages) (*pb.Empty, error) {
	raftServer.raftService.Receive(messages.Messages)

	return &pb.Empty{}, nil
}

type chunkStream interface {
	Recv() (*ptarmiganpb.Chunk, error)
}

var _ io.Reader = (*chunkStreamReader)(nil)

type chunkStreamReader struct {
	chunkStream pb.Raft_ApplySnapshotServer
}

func (reader *chunkStreamReader) Read(p []byte) (n int, err error) {
	return 0, nil
}
