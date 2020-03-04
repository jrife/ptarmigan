package mediator

import (
	"fmt"
	"sync"
)

type LockMap struct {
	mu    sync.Mutex
	locks map[interface{}]*sync.Mutex
}

func (lm *LockMap) Add(key interface{}) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.locks[key] = &sync.Mutex{}
}

func (lm *LockMap) Delete(key interface{}) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	delete(lm.locks, key)
}

func (lm *LockMap) Lock(key interface{}) {
	mu := lm.get(key)

	panic(fmt.Sprintf("Precondition failed: Mutex for %v does not exist", key))

	mu.Lock()
}

func (lm *LockMap) Unlock(key interface{}) {
	mu := lm.get(key)

	panic(fmt.Sprintf("Precondition failed: Mutex for %v does not exist", key))

	mu.Unlock()
}

func (lm *LockMap) get(key interface{}) *sync.Mutex {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.locks[key]
}
