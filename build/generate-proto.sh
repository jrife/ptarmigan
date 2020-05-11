#!/bin/bash
set -x

PTARMIGAN_ROOT=/home/me/ptarmigan
GOGOPROTO_ROOT=/home/me/protobuf
GRPC_GATEWAY_ROOT=/home/me/grpc-gateway
cd $PTARMIGAN_ROOT/transport/ptarmiganpb
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=. --gogofaster_out=paths=source_relative:. ptarmigan.proto
cd $PTARMIGAN_ROOT/transport/frontends/grpc/pb
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=. --gogofaster_out=plugins=grpc:. ptarmigan.proto
cd $PTARMIGAN_ROOT/storage/kv/pb
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=. --gogofaster_out=paths=source_relative:. kv.proto
cd $PTARMIGAN_ROOT/flock/server/flockpb
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=$GRPC_GATEWAY_ROOT/third_party/googleapis -I=. --gogofaster_out=plugins=grpc:. flock.proto
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=$GRPC_GATEWAY_ROOT/third_party/googleapis -I=. --gogofaster_out=plugins=grpc:. rpc.proto
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=$GRPC_GATEWAY_ROOT/third_party/googleapis -I=. --grpc-gateway_out=logtostderr=true:. rpc.proto
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=$GRPC_GATEWAY_ROOT/third_party/googleapis -I=. --swagger_out=logtostderr=true:. rpc.proto
