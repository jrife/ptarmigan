package controllers

import "context"

// Coordinator participates in leader elections with other flock
// coordinator hosts and runs controllers when it is elected leader
type Coordinator struct {
}

// Run starts the coordinator
func (coordinator *Coordinator) Run(ctx context.Context) {
	for term := range coordinator.campaign(ctx) {
		coordinator.run(term)
	}
}

func (coordinator *Coordinator) run(ctx context.Context) {
}

func (coordinator *Coordinator) campaign(ctx context.Context) <-chan context.Context {
	// When there exists no fencing token for fencing group
	// 1) Create a fencing token for group
	// 2) Start controllers with this fencing token
	// When a higher fencing token is assgigned to the group
	// 1) Stop controllers
	return nil
}
