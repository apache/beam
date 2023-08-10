#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

read -r -d '\0' LICENSE << EOM
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
\0
EOM

if [[ -z "$(which protoc)" ]]; then
  echo "protoc not found on path"
  exit 1
fi

SCRIPT_DIR="$( realpath "$( dirname "${BASH_SOURCE[0]}" )" )"
if [[ -z $SCRIPT_DIR ]]; then
  echo "unable to resolve path to script"
  exit 1
fi

SDK_PATH="$( realpath "$(dirname $SCRIPT_DIR)/.." )"
if [[ -z "$SDK_PATH" ]]; then
  echo "unable to resolve path to project root"
  exit 1
fi

PROJECT_ROOT="$(realpath "$(dirname $SCRIPT_DIR)/../..")"
if [[ -z "$PROJECT_ROOT" ]]; then
  echo "unable to resolve path to project root"
  exit 1
fi

# You can manually specify this otherwise we assume we are in the repo
if [[ -z "$BEAM_PATH" ]]; then
    BEAM_PATH=${PROJECT_ROOT}
fi

function gen_beam_model_protos() {
  cd "$PROJECT_ROOT"
  echo "Generating Beam Model Protos in $PROJECT_ROOT from $BEAM_PATH"

  declare -a TO_GEN=(
    '/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/*.proto'
    '/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/*.proto'
    '/model/fn-execution/src/main/proto/org/apache/beam/model/fn_execution/v1/*.proto'
  )

  declare -a INCLUDES=()
  for package in "${TO_GEN[@]}"
  do
    INCLUDES+=("-I${BEAM_PATH}/${package%/org*}")
  done

  mkdir -p $PROJECT_ROOT/sdks/swift/Sources/ApacheBeam/Generated/{Model,GRPC}
  for package in "${TO_GEN[@]}"
  do
    echo "${INCLUDES[@]}"
    protoc \
      "${INCLUDES[@]}" \
      --swift_out="$PROJECT_ROOT/sdks/swift/Sources/ApacheBeam/Generated/Model" \
      --grpc-swift_out="$PROJECT_ROOT/sdks/swift/Sources/ApacheBeam/Generated/GRPC" \
      ${BEAM_PATH}$package
  done
}

gen_beam_model_protos
