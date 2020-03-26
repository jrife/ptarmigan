// Package transport contains descriptions of all the
// services exposed by a ptarmigan server and different
// implementations of clients and servers for different
// protocols. The idea is that some clients may prefer
// gRPC, REST, or something else in the future. We want
// to make it easy to add support for new transports without
// too much refactoring.
package transport
