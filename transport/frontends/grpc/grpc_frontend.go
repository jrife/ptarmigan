package grpc

import (
	"net"

	"github.com/jrife/ptarmigan/transport/frontends/grpc/pb"

	"github.com/jrife/ptarmigan/transport"
	"github.com/jrife/ptarmigan/transport/frontends"
	"google.golang.org/grpc"
)

// Frontend is an implementation of
// PtarmiganFrontend for the gRPC protocol
type Frontend struct {
	ptarmiganServer transport.PtarmiganServer
	grpcServer      *grpc.Server
}

// Init initializes the frontend
func (frontend *Frontend) Init(options frontends.Options) error {
	frontend.ptarmiganServer = options.Server
	frontend.grpcServer = grpc.NewServer()

	pb.RegisterRaftServer(frontend.grpcServer, &RaftServer{raftService: frontend.ptarmiganServer})

	return nil
}

// Listen accepts connections from this listener
func (frontend *Frontend) Listen(listener net.Listener) error {
	return nil
}

// Stop stops accepting connections from listeners and causes
// all calls to Listen to return
func (frontend *Frontend) Stop() error {
	return nil
}
