#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
    $MODEL_PROTOS/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/*.proto        \

run npx protoc --ts_out . \
    --ts_opt client_grpc1,generate_dependencies \
    --proto_path $MODEL_PROTOS/pipeline/src/main/proto/        \
    --proto_path $MODEL_PROTOS/job-management/src/main/proto/  \
    $MODEL_PROTOS/job-management/src/main/proto/org/apache/beam/model/job_management/v1/*.proto        \

# Need the server for the loopback worker.
run npx protoc --ts_out . \
    --ts_opt client_grpc1,server_grpc1,generate_dependencies              \
    --proto_path $MODEL_PROTOS/pipeline/src/main/proto/      \
    --proto_path $MODEL_PROTOS/fn-execution/src/main/proto/  \
    $MODEL_PROTOS/fn-execution/src/main/proto/org/apache/beam/model/fn_execution/v1/*.proto        \

# Make old-style aliases for simple imports.
python <<EOF

import glob
import os

for ts_file in glob.glob("org/apache/beam/model/*/v1/*.ts"):
  with open(os.path.basename(ts_file), 'w') as fout:
    print("Creating alias for", ts_file)
    fout.write('export * from "./%s"\n' % ts_file[:-3])

EOF
