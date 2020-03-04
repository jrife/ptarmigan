protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf/ -I=$GOPATH/src/github.com/gogo/protobuf --gogofaster_out=. state_maching.proto
