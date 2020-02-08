package controllers

// Nodes
// Keyspace
// Raft Group
// Raft Group Replica

// Add Node - Assign raft group replicas to nodes
// Remove Node - Assign raft group replicas to nodes
// Add Keyspace - Create raft group, assign raft group replicas to nodes
// Remove Keyspace - Delete raft group, delete raft group replicas from nodes

// Keyspaces are created or deleted by users
// replica_controller: Creates raft groups for newly created keyspaces. Tears down raft groups
// when keyspaces are deleted
// replica_scheduler: Assigns raft group replicas to nodes. Takes into account availability zone distribution,
// node capacity, etc. Automatic rebalancing could be handled by this controller if enabled
