#!/bin/bash

# Once: npm install @protobuf-ts/plugin

MODEL_PROTOS=../../../../../model

echo $MODEL_PROTOS
ls $MODEL_PROTOS

npx protoc --ts_out . \
    --proto_path $MODEL_PROTOS/pipeline/src/main/proto/  \
    $MODEL_PROTOS/pipeline/src/main/proto/*.proto        \

npx protoc --ts_out . \
    --proto_path $MODEL_PROTOS/pipeline/src/main/proto/        \
    --proto_path $MODEL_PROTOS/job-management/src/main/proto/  \
    $MODEL_PROTOS/job-management/src/main/proto/*.proto        \

npx protoc --ts_out . \
    --proto_path $MODEL_PROTOS/pipeline/src/main/proto/      \
    --proto_path $MODEL_PROTOS/fn-execution/src/main/proto/  \
    $MODEL_PROTOS/fn-execution/src/main/proto/*.proto        \
