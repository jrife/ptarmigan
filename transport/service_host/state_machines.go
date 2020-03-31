package service_host

// Simplified view. Just brainstorming
type PtarmiganServiceHost interface {
	Put(partition, key, value string) error
	Get(partition, key, value string) error
}

// Frontend -> ServiceHost (interface) ->
// Hierarchy of State Machine:                  Alternate Names
// 1. State Machine Type (Ptarmigan, Nop, Etc.) (Storage Class)
// 2. State Machine Instance                    (Store)
// 3. State Machine Partition                   (Partition)
//
// Every node is aware of all supported state machine types
// Imagine a Stateful Servive (storage class/whatever) as a different entity (like another process)
// How could somebody build a completely new service without modifying the software? That will
// help us decouple the design
//
