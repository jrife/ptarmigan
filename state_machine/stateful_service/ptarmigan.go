package stateful_service

import "io"

type PtarmiganServiceProvider struct {
}

func (serviceProvider *PtarmiganServiceProvider) CreateStore() {
}

func (serviceProvider *PtarmiganServiceProvider) DeleteStore() {
}

func (serviceProvider *PtarmiganServiceProvider) Step(store, partition, replica, message string) error {
	return nil
}

func (serviceProvider *PtarmiganServiceProvider) ApplySnapshot(store, partition, replica, snap io.Reader) error {
	return nil
}

func (serviceProvider *PtarmiganServiceProvider) Snapshot(store, partition, replica string) (io.Reader, error) {
	return nil, nil
}
