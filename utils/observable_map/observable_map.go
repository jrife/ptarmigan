package observable_map

import "sync"

// MapObserver is a callback through which observers
// can be notified of map changes.
type MapObserver func(key interface{}, value interface{})

// ObservableMap is a thread-safe wrapper for go's
// map that allows observers to be notified when
// things are added or deleted
type ObservableMap struct {
	mu              sync.Mutex
	internalMap     map[interface{}]interface{}
	addObservers    []MapObserver
	updateObservers []MapObserver
	deleteObservers []MapObserver
}

// New creates an empty ObservableMap
func New() *ObservableMap {
	return &ObservableMap{
		internalMap:     make(map[interface{}]interface{}),
		addObservers:    make([]MapObserver, 0),
		updateObservers: make([]MapObserver, 0),
		deleteObservers: make([]MapObserver, 0),
	}
}

// Put sets a key in the map. It returns true
// if the key already existed in the map and
// false if this call to Put is adding a key
// that didn't exist before.
func (observableMap *ObservableMap) Put(key, value interface{}) bool {
	observableMap.mu.Lock()

	_, ok := observableMap.internalMap[key]
	observableMap.internalMap[key] = value

	observableMap.mu.Unlock()

	// If the key already exists this is an update
	// Otherwise it's an add
	if ok {
		observableMap.notifyObservers(observableMap.updateObservers, key, value)
	} else {
		observableMap.notifyObservers(observableMap.addObservers, key, value)
	}

	return ok
}

// Delete deletes a key from the map. It returns
// true if the key existed in the map and false
// if the key didn't exist.
func (observableMap *ObservableMap) Delete(key interface{}) bool {
	observableMap.mu.Lock()

	value, ok := observableMap.internalMap[key]

	// Only notify observers if we're actually
	// removing something that exists
	if ok {
		delete(observableMap.internalMap, key)
		observableMap.notifyObservers(observableMap.deleteObservers, key, value)
	}

	observableMap.mu.Unlock()

	return ok
}

// Get reads a key from the map. If the key exists
// its value will be returned and ok will be true
// If the value doesn't exist a nil value will be
// returned and ok will be false.
func (observableMap *ObservableMap) Get(key interface{}) (interface{}, bool) {
	observableMap.mu.Lock()
	defer observableMap.mu.Unlock()

	value, ok := observableMap.internalMap[key]

	return value, ok
}

func (observableMap *ObservableMap) notifyObservers(observers []MapObserver, key, value interface{}) {
	for _, observer := range observers {
		observer(key, value)
	}
}

// OnAdd registers an observer for when a new key is
// added to the map.
func (observableMap *ObservableMap) OnAdd(cb MapObserver) {
	observableMap.mu.Lock()
	defer observableMap.mu.Unlock()

	observableMap.addObservers = append(observableMap.addObservers, cb)
}

// OnUpdate registers an observer for when an existing
// key is updated.
func (observableMap *ObservableMap) OnUpdate(cb MapObserver) {
	observableMap.mu.Lock()
	defer observableMap.mu.Unlock()

	observableMap.updateObservers = append(observableMap.updateObservers, cb)
}

// OnDelete registers an observer for map deletes.
func (observableMap *ObservableMap) OnDelete(cb MapObserver) {
	observableMap.mu.Lock()
	defer observableMap.mu.Unlock()

	observableMap.deleteObservers = append(observableMap.deleteObservers, cb)
}
