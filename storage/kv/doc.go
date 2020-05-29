// Package kv provides an interface for implementing
// kv drivers that can be used to build more complex storage
// interfaces.
//
// A kv plugin is a factory for root store instances. A root store
// contains zero or more stores and stores contain zero or more
// partitions. Each store operates independently from other stores. Likewise,
// partitions within a store operate independently from other partitions.
// Transactions for different partitions are completely independent from each
// other: there are no ordering or consistency guarantees for transactions spawned
// from different partitions. Within a partition  transactions are stricly serializable.
//
//  - Root Store
//    - Store A
//      - Partition 1
//        - key1: abc
//        - key2: def
//      - Partition 2
//    - Store B
//      - Partition 1
//      - Partition 2
//        - keyN: aaa
//        - keyM: xyz
//      - Partition 3
//    - Store C
//      - Partition 1
//
// Rather than defining a flat interface that allows a user to perform transactions
// over a list of key-value pairs, partitioning was pushed down to this layer
// to enable the kv drivers to make more intelligent decisions on how to concurrently
// run transactions across different partitions and to more accurately model the use
// cases of the layers above this.
//
// A three-level hierarchy was chosen rather than a two level hierarchy such as store/partition
// mainly for convenience. Each store acts like a namespace, allowing different components that
// require a kv storage interface to have their own store without needing to worry about stepping
// on the toes of other components.
package kv
