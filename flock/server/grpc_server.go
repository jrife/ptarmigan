package server

import (
	"net"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
	"google.golang.org/grpc"
)

type GRPC struct {
	grpcServer *grpc.Server
}

func NewGRPC(host *FlockHost) *GRPC {
	g := &GRPC{
		grpcServer: grpc.NewServer(),
	}

	flockpb.RegisterKVServer(g.grpcServer, host.KV())
	flockpb.RegisterLeasesServer(g.grpcServer, host.Leases())

	return g
}

func (grpcServer *GRPC) Listen(listener net.Listener) error {
	return grpcServer.grpcServer.Serve(listener)
}
