package server

import (
	"context"

	"github.com/jrife/ptarmigan/flock/server/flockpb"
)

// It would probably be easiest to fix the master replicas
// in place. Never move existing replicas, just grow or shrink
// the set with some protections. Make sure the flock storage class
// is internal not external.
// * Add Master Replicas: Safe because watchers learn from existing masters
// * Remove Master Replicas: Safe because watchers learn from remaining members
// Static bool // indicate whether or not a replica can be moved to a new host
// Master Controllers ensure that ptarmigan nodes are informed of master changes.
// Non-static master may be feasible with master store replicas co-located with their raft replicas
// Master nodes (with ptarmigan and flock) can notify ptarmigans of master host changes

// REPLICA HOST RESOLVER: On which flock hosts do replicas for the partition addressed by this query reside?
// LookupHosts(Partition) -> {HOST1, HOST2, HOST3}
// Flock host asks a ptarmigan host
// Ptarmigan host asks a flock host with a master store replica
// All ptarmigan hosts need to know where master store replicas reside
// If master store replicas are moved to new hosts, are deleted, or when new ones are added ptarmigan hosts need to be aware
// Assuming no caching:
// Each ptarmigan node has this stored locally:
// Master Store Replica Hosts: { R1: 'a.b.c:8080', R2: 'a.b.d:8181', R3: 'a.b.e:8080' }
// It may be out of date. With static master allocation this won't happen but with
// master replica rescheduling it becomes an issue. Can we quantify how out of date
// a single ptarmigan replica is allowed to be with respect to the current master replica
// to host mapping and construct our scheduler in such a way that no ptarmigan node ever
// falls too behind?
// For non-linearizable queries:
// * As long as each ptarmigan node is correct about at least one master store replica location
//   they can fulfill the request, assuming those hosts are healthy.
// Kinds of Operations Which Require Moving Master Replicas To New Host:
// * Decommission Node
// * Add Node (Rebalancing)
// Kinds of Operations Which Require Provisioning New Master Replicas:
// * Remove Node
// * Decrease Replication Factor
//
// Initial Settings:
// All ptarmigans aware { Master1: Host A }
// Now i want to move master1 to host B for some reason
// I need to notify all ptarmigans of my intent to move Master1 before moving it since they actually need to learn
// from the master where the master is (going to be)
//
//
// For linearizable queries:
//
// 1) Flock Host A     -> Ptarmigan Host X : LookupHosts(Partition)
// 2) Ptarmigan Host X -> Flock Host B     : Query(To Find Partition Hosts, linearizable=true)
// 3) Flock Host B     -> Ptarmigan Host Y : ReadIndex(raftGroup) (if query must be linearizable)
// 4) Ptarmigan Host Y -> Flock Host B     : Query(To Find Raft Group Hosts) (to send raft messages to fulfill readindex request) (may tolerate stale reads)
// This looks cylcic if both queries are linearizable...
// Consequences of stale reads at step 4?
// * It could mean that some raft replicas appear to be unreachable since we have their old address.
//
//
// 1) Send hosts lookup lookup request to a ptarmigan node
// 2) Ptarmigan node sends non-linearizable read to a flock node where a master replica resides
//   a) what if it can't reach that replica? does it try another one?
//   b) what if its knowledge of master node locations is out of date?
//   c) How does each ptarmigan node know which hosts the master store's replicas reside on? static?
// 3) Ptarmigan node responds to the request after receiving response from master flock store.
//
// All ptarmigan hosts know where all master nodes are
// During transitions the controller ensures that all ptarmigan nodes
// have received the update to master replica membership before moving on
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
