package server

import (
	"context"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
)

type FlockHost struct {
}

func (host *FlockHost) KV() flockpb.KVServer {
	return &FlockHostKV{}
}

func (host *FlockHost) Leases() flockpb.LeasesServer {
	return nil
}

type FlockHostKV struct {
}

// Query retrieves kvs from a partition
func (host *FlockHostKV) Query(context.Context, *flockpb.KVQueryRequest) (*flockpb.KVQueryResponse, error) {
	// ROUTE TO CORRECT NODE
	return nil, nil
}

// Txn processes multiple requests in a single transaction.
// A txn request increments the revision of the key-value store
// and generates events with the same revision for every completed request.
// It is not allowed to modify the same key several times within one txn.
func (host *FlockHostKV) Txn(context.Context, *flockpb.KVTxnRequest) (*flockpb.KVTxnResponse, error) {
	return nil, nil
}

// Compact compacts the event history in the key-value store. The key-value
// store should be periodically compacted or the event history will continue to grow
// indefinitely.
func (host *FlockHostKV) Compact(context.Context, *flockpb.KVCompactionRequest) (*flockpb.KVCompactionResponse, error) {
	return nil, nil
}

// Watch creates a watcher to watch for events happening or that have happened.
// The entire event history can be watched starting from the last compaction revision.
func (host *FlockHostKV) Watch(*flockpb.KVWatchRequest, flockpb.KV_WatchServer) error {
	return nil
}

type FlockHostLeases struct {
}

// Grant creates a lease which expires if the server does not receive a renewal
// within a given time to live period. All keys attached to the lease will be expired and
// deleted if the lease expires. Each expired key generates a delete event in the event history.
func (host *FlockHostLeases) Grant(context.Context, *flockpb.LeaseGrantRequest) (*flockpb.LeaseGrantResponse, error) {
	return nil, nil
}

// Query lists leases
func (host *FlockHostLeases) Query(context.Context, *flockpb.LeaseQueryRequest) (*flockpb.LeaseQueryResponse, error) {
	return nil, nil
}

// Get retrieves information on a lease.
func (host *FlockHostLeases) Get(context.Context, *flockpb.LeaseGetRequest) (*flockpb.LeaseGetResponse, error) {
	return nil, nil
}

// Revoke revokes a lease. All keys attached to the lease will expire and be deleted.
func (host *FlockHostLeases) Revoke(context.Context, *flockpb.LeaseRevokeRequest) (*flockpb.LeaseRevokeResponse, error) {
	return nil, nil
}

// Renew renews the lease so that it does not expire
func (host *FlockHostLeases) Renew(context.Context, *flockpb.LeaseRenewRequest) (*flockpb.LeaseRenewResponse, error) {
	return nil, nil
}
