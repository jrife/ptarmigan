package mvcc

var _ IStore = (*Store)(nil)

// Store implements IStore. It is the
// top object for accessing storage for
// a flock node
type Store struct {
}

// ReplicaStores lets a consumer iterate through
// all the replica stores for this node. Replica
// stores are returned in order by their name.
// Returns a list of IReplicaStores whose name is
// > start up to the specified limit.
func (store *Store) ReplicaStores(start string, limit int) ([]IReplicaStore, error) {
	return nil, nil
}

// ReplicaStore returns a handle to the replica
// store with the given name. Calling methods on
// the handle will return ErrNoSuchReplicaStore
// if it hasn't been created.
func (store *Store) ReplicaStore(name string) IReplicaStore {
	return &ReplicaStore{}
}
