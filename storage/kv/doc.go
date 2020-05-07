// Package kv provides an interface and specification for implementing
// KV driver plugins that can be used to build more complex storage
// interfaces.
//
// A kv plugin is essentially a factory for instances of RootStore
// The kv interface breaks a kv store into three levels: A root store,
// a store, and a partition. A root store is the top level container that
// is made up of various stores. Each store is independent
// from other stores belonging to the same root store, but stores are bound
// to the lifecycle of their root store. In other words, if a root store
// is closed or deleted this closes or deletes its stores as well. Stores are
// further divided into partitions. The relationship between partition and store
// is similar to that between store and root store: if a store is closed or
// deleted its partitions are also closed or deleted. Partitions contain some
// metadata, which can contain information about that partition, and a sorted
// table of key-value pairs. Transactions for different partitions are completely
// independent from each other: there are no ordering or consistency guarantees
// for transactions spawned from different partitions.
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
// cases of the layers above this. An example of how partition-awareness might be used
// for better is a root store that multiplexes stores or partitions over multiple boltdb
// databases to avoid a transaction on one partition blocking a transaction on another
// partition.
//
// Rather than having a two level hierarchy such as store/partition it was chosen that
// there would be the three level hierarchy described above: root store/store/partition.
// The reason for this is mainly convenience. Each store acts like a namespace, allowing
// different components to be handed an instance of a store so that different components
// that need to store things don't need to worry about managing different sets of partitions
// themselves.
package kv
