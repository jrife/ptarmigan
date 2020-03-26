package transport

import (
	"github.com/jrife/ptarmigan/transport/services"
)

// PtarmiganClient describes the interface
// for public clients of a ptarmigan cluster.
// In other words, this interface describes
// all operations a user of a ptarmigan cluster
// may want to perform.
type PtarmiganClient interface {
}

// PtarmiganClientInternal describes the interface
// for internal clients such as other ptarmigan nodes.
// It adds on operations that are relevant only between
// nodes in the cluster.
type PtarmiganClientInternal interface {
	PtarmiganClient
	services.RaftClient
}

// PtarmiganServer describes an interface
// that will be passed to each type of
// frontend. Each frontend provides support
// for a different type of protocol. The
// idea here is to decouple the inner
// workings of a ptarmigan node from the
// protocol that they use to communicate
type PtarmiganServer interface {
	services.RaftService
}
