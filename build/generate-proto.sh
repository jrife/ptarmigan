#!/bin/bash
set -x

FLOCK_ROOT=/home/me/flock
GOGOPROTO_ROOT=/home/me/protobuf
GRPC_GATEWAY_ROOT=/home/me/grpc-gateway
cd $FLOCK_ROOT/storage/kv/pb
protoc -I=$FLOCK_ROOT/vendor/ -I $FLOCK_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=. --gogofaster_out=paths=source_relative:. kv.proto
cd $FLOCK_ROOT/storage/kv/composite/pb
protoc -I=$FLOCK_ROOT/vendor/ -I $FLOCK_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=. --gogofaster_out=paths=source_relative:. node.proto
cd $FLOCK_ROOT/ptarmigan/server/ptarmiganpb
protoc -I=$FLOCK_ROOT/vendor/ -I $FLOCK_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=$GRPC_GATEWAY_ROOT/third_party/googleapis -I=. --gogofaster_out=plugins=grpc:. ptarmigan.proto
protoc -I=$FLOCK_ROOT/vendor/ -I $FLOCK_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=$GRPC_GATEWAY_ROOT/third_party/googleapis -I=. --gogofaster_out=plugins=grpc:. rpc.proto
protoc -I=$FLOCK_ROOT/vendor/ -I $FLOCK_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=$GRPC_GATEWAY_ROOT/third_party/googleapis -I=. --grpc-gateway_out=logtostderr=true:. rpc.proto
protoc -I=$FLOCK_ROOT/vendor/ -I $FLOCK_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=$GRPC_GATEWAY_ROOT/third_party/googleapis -I=. --swagger_out=logtostderr=true:. rpc.proto
