package grpc

import (
	"net"

	"github.com/jrife/ptarmigan/transport/service_host"

	"github.com/jrife/ptarmigan/transport/frontends/grpc/pb"

	"github.com/jrife/ptarmigan/transport/frontends"
	"google.golang.org/grpc"
)

// Frontend is an implementation of
// PtarmiganFrontend for the gRPC protocol
type Frontend struct {
	host       service_host.PtarmiganHost
	grpcServer *grpc.Server
}

// Init initializes the frontend
func (frontend *Frontend) Init(options frontends.Options) error {
	frontend.host = options.Host
	frontend.grpcServer = grpc.NewServer()

	pb.RegisterRaftServer(frontend.grpcServer, &RaftServer{host: frontend.host})

	return nil
}

// Listen accepts connections from this listener
func (frontend *Frontend) Listen(listener net.Listener) error {
	return frontend.grpcServer.Serve(listener)
}

// Stop stops accepting connections from listeners and causes
// all calls to Listen to return
func (frontend *Frontend) Stop() error {
	frontend.grpcServer.GracefulStop()

	return nil
}
