package service_host

import "github.com/jrife/ptarmigan/stateful_services"

// PtarmiganHost describes an interface
// that will be passed to each type of
// frontend. Each frontend provides support
// for a different type of protocol. The
// idea here is to decouple the inner
// workings of a ptarmigan node from the
// protocol that they use to communicate
type PtarmiganHost interface {
	NodeServiceHost
	RaftServiceHost
	stateful_services.StatefulServiceHost
}
