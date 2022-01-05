#!/bin/bash

# Once: npm install @protobuf-ts/plugin

MODEL_PROTOS=../../../../../model

run() {
  echo
  echo $*
  $*
}

run npx protoc --ts_out . \
    --ts_opt client_grpc1,generate_dependencies \
    --proto_path $MODEL_PROTOS/pipeline/src/main/proto/  \
    $MODEL_PROTOS/pipeline/src/main/proto/*.proto        \

run npx protoc --ts_out . \
    --ts_opt client_grpc1,generate_dependencies \
    --proto_path $MODEL_PROTOS/pipeline/src/main/proto/        \
    --proto_path $MODEL_PROTOS/job-management/src/main/proto/  \
    $MODEL_PROTOS/job-management/src/main/proto/*.proto        \

# Need the server for the loopback worker.
run npx protoc --ts_out . \
    --ts_opt client_grpc1,server_grpc1,generate_dependencies              \
    --proto_path $MODEL_PROTOS/pipeline/src/main/proto/      \
    --proto_path $MODEL_PROTOS/fn-execution/src/main/proto/  \
    $MODEL_PROTOS/fn-execution/src/main/proto/*.proto        \
