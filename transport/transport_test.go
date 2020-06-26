package transport_test

import "testing"

type Protocol interface {
	Client
}

// These tests check compliance with common requirements
// for different client-frontend implementations such as
// error codes, input validation, etc.
func TestTransports(t *testing.T) {

}
