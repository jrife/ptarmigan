package grpc

import (
	"github.com/jrife/ptarmigan/transport/clients"
)

type Config struct {
}

func New(config Config) clients.PtarmiganClient {
	return nil
}

func NewInternal(config Config) clients.PtarmiganClientInternal {
	return nil
}
