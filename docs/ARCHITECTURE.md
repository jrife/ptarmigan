Scale-out distributed coordination service

## Why
* Multi-tenant shared-nothing systems
* Lower management overhead
* Increase compute density

## Design Philosophy
* Easy cluster operation
* Layered approach
* AZ aware partition assignment
* "High-Availability" - The minority half of a partition would never have "availability" in the academic sense. We mean to handle one or more crash-stop failures without
  an outside observer seeing the system as unavailable
* Controller + Model
* Easy to rollback failed cluster changes, easy to disagnose problems with cluster changes, easy to diagnose problems with cluster health

## Nodes
Master nodes
Keyspace nodes

## Features
* Data model based on etcd (directory + file)
* Adds partitioning where each partition is its own Raft group
* Sequential consistency and Linearizability
* One master node per AZ, master nodes form 1 raft group. Master raft group is used to store cluster metadata like node-replica assignment.
  i.e. there exists a single "CLUSTER" keyspace that has the same interface as any user-facing keyspace that the cluster itself uses for config.
* Replica assignment for each raft group ensures one replica per AZ
* "Cluster controller" - partition-node assignment, rebalancing

## Cluster Metadata
Captures the cluster's operating state as a set of collections and relationships
between collection objects
* Nodes
* Keyspaces
* Raft Groups
* Keyspace to Raft Group Mapping (Desired + Current)
* Raft Group Replica to Node Mapping (Desired + Current)

## Cluster Management Controller
* Replica allocation: decide where to place replicas, 

## Failure Modes That We Should Tolerate (liveness may not be guaranteed)
* Network partitions
* Fail-stop faults

## Safety Properties
* Loosely speaking, state transitions should always be resumable if some step fails or is interrupted
* Cluster should not enter a corrupted state if a node fails during state transition.

## Liveness Properties

### Bootstrapping a Raft Group
1. A new raft group is created
2. A cluster controller sees that a new raft group was created, creates raft group replicas, and assigns them to nodes

### Adding a Node To The Cluster
1. A new node adds itself to Nodes in the cluster metadata
2. A cluster controller decides to move some partition replicas to this new Node to even out load. It changes the desired node
3. When a node learns that one of its raft group replicas needs to be moved elsewhere it pauses that replica
   so that its state is not being changed during the transfer.
4. When the new node learns that it needs a replica from another node it asks the old node for the replica data. The old node will
   reject the request if it has not yet learned of the reassignment. (transfer implies that the replica state is locked)
5. The new node updates the cluster metadata indicating that it is now the current home for that replica

### Removing a Node From The Cluster (Graceful)
Cluster downsizing normal
1. A node is marked for decomissioning
2. A cluster controller decides to move its partition replicas to other Nodes. It changes the desired node
3. After this the partition transfer procedure is similar to adding a node.
4. A cluster controller learns that a node marked for deletion has transferred all its replica elsewhere. The node is deleted
   from metadata

### Removing a Node From Cluster (Forceful)
A node has permanently gone offline (disk data not recoverable)
1. Any raft group replica that existed on the failed node needs to be replaced with a brand-new raft group replica.
2. The node is deleted from cluster metadata with some user request
3. Some cluster controller learns that raft group replicas are assigned to a non-existent node.
4. The cluster controller creates a new raft group replica inside affected raft groups to meet their replication requirements and assigns it to some existing node
5. A node learns that it has been assigned a raft group replica that does not currently reside anywhere (current is empty). This indicates that it needs to follow
   a specialized setup procedure
6. The node initializes a new raft group replica and it joins that raft group
7. The node updates the current location

### How Cluster Operations Work For Master Node?
Is it any different than any other raft group/keyspace?

### Leader Election
* Some of the algorithms listed above make the assumption that only
  one leader is reading and writing cluster metadata at once.
* Fencing may be necesssary to prevent concurrent updates from breaking global invariants:
  i.e. ensuring replicas for a keyspace all exist on different nodes

## API
* Create
* Set
* Delete
* CompareAndSwap
* CompareAndDelete
* Get
* Watch

## State Layers
How data structures are layered
* Raft Group
* Log
* Keyspace

## Control Loops
* 
* 

Keyspaces have a "disruption budget"

Cluster management operations:
- Add Node
- Remove Node
- Decomission Node
- Initialize Keyspace
- Delete Keyspace
- Rebalance
- Backup/Restore - How to take consistent snapshot? Depends