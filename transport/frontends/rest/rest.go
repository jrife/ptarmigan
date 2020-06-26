package rest

import (
	"fmt"
	"net"

	"github.com/jrife/ptarmigan/transport"
	"github.com/jrife/ptarmigan/transport/frontends"
)

// Frontend is an implementation of
// PtarmiganFrontend for REST
type Frontend struct {
	server transport.PtarmiganServer
}

// Init initializes the frontend
func (frontend *Frontend) Init(options frontends.Options) error {
	frontend.server = options.Server

	return fmt.Errorf("Not implemented")
}

// Listen accepts connections from this listener
func (frontend *Frontend) Listen(listener net.Listener) error {
	return fmt.Errorf("Not implemented")
}

// Stop stops accepting connections from listeners and causes
// all calls to Listen to return
func (frontend *Frontend) Stop() error {
	return fmt.Errorf("Not implemented")
}
