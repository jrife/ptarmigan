package stateful_service

import "io"

type ExternalServiceProvider struct {
}

func (serviceProvider *ExternalServiceProvider) CreateStore() {
}

func (serviceProvider *ExternalServiceProvider) DeleteStore() {
}

func (serviceProvider *ExternalServiceProvider) Step(store, partition, replica, message string) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) ApplySnapshot(store, partition, replica, snap io.Reader) error {
	return nil
}

func (serviceProvider *ExternalServiceProvider) Snapshot(store, partition, replica string) (io.Reader, error) {
	return nil, nil
}

func (serviceProvider *ExternalServiceProvider) LastAppliedIndex(store, partition, replica string) (int, error) {
	return 0, nil
}
