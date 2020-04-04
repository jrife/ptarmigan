package clients

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
	RaftServiceClient
	StatefulServicesClient
}
