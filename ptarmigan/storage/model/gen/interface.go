package gen

import (
	"github.com/jrife/flock/ptarmigan/storage"
)

// ReplicaStoreSet can be implemented by a system under test
// to test a set of replica stores at the same time
type ReplicaStoreSet interface {
	Get(i int) storage.ReplicaStore
}
