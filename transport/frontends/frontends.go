package frontends

import (
	"net"

	"github.com/jrife/ptarmigan/transport"
)

// Options define standard options
// passed to frontends during initialization
type Options struct {
	Server  transport.PtarmiganServer
	Options map[string]interface{}
}

// PtarmiganFrontend describes an interface
// that every ptarmigan frontend must
// implement.
type PtarmiganFrontend interface {
	// Init initializes the frontend. Use this
	// to pass configuration options to the frontend
	Init(options Options) error
	// Listen tells this frontend to start listening
	// using this listener. A frontend may be asked
	// to listen on different interfaces, such as a TCP
	// socket and a Unix socket. It must accept
	// one or more calls to Listen. Listen must block
	// as long as it is actively accepting connections
	// from this listener. If the listener returns an
	// error Listen must return an error and return. If
	// Listen returns as a result of Stop being called it
	// must return nil.
	Listen(listener net.Listener) error
	// Stop tells this frontend to stop processing all
	// requests and stop listening to all listeners.
	// It must not close the listeners, however. That
	// is not its responsibility.
	Stop() error
}
