package service_host

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
	StatefulServiceProvidersHost
}
