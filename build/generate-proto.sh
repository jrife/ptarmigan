#!/bin/bash
set -x

PTARMIGAN_ROOT=/home/me/ptarmigan
GOGOPROTO_ROOT=/home/me/protobuf
cd $PTARMIGAN_ROOT/transport/ptarmiganpb
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=. --gogofaster_out=paths=source_relative:. ptarmigan.proto
cd $PTARMIGAN_ROOT/transport/frontends/grpc/pb
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=. --gogofaster_out=plugins=grpc:. ptarmigan.proto
cd $PTARMIGAN_ROOT/storage/kv/kvpb
protoc -I=$PTARMIGAN_ROOT/vendor/ -I $PTARMIGAN_ROOT -I=$GOGOPROTO_ROOT/protobuf/ -I=$GOGOPROTO_ROOT -I=. --gogofaster_out=paths=source_relative:. kv.proto
