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

if [[ -z "$(which go)" ]]; then
  echo "go runtime missing"
  exit 1
fi

if [[ -z "$GOBIN" ]]; then
  export GOBIN="$(go env GOPATH)/bin"
fi

export PATH="$PATH:$GOBIN"
GEN_DIR=$PWD

# NB: Keep these two versions in sync with those defined in
#     the go.mod.
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@ebf6a4b
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1

function insert_license_header() {
  local depth="$1"
  # protoc-gen-go-grpc does not yet output comments from the original
  # proto file so we need to manually add the license header.
  while IFS= read -d $'\0' -r file ; do
    tmp_file=$(mktemp)
    echo "$LICENSE" > $tmp_file
    cat $file >> $tmp_file
    mv $tmp_file $file
  done < <(find $GEN_DIR $depth -iname "*grpc.pb.go" -print0)
}

function gen_go_sdk_protos() {
  LIBRARY_PATH="${PWD##${SDK_PATH}/}"
  PROTOS="$LIBRARY_PATH/*.proto"

  cd "$SDK_PATH"

  # TODO: make this more data driven.
  PKG_MAP=Mbeam_fn_api.proto=github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1

  declare -a TO_INC=(
    "."
    "$PROJECT_ROOT/model/job-management/src/main/proto/"
    "$PROJECT_ROOT/model/pipeline/src/main/proto/"
    "$PROJECT_ROOT/model/fn-execution/src/main/proto/"
  )

  declare -a INCLUDES=()
  for package in "${TO_INC[@]}"
  do
    INCLUDES+=("-I${package}")
  done

  protoc \
    "${INCLUDES[@]}" \
    --go_opt=module=github.com/apache/beam/sdks/v2 \
    --go-grpc_opt=module=github.com/apache/beam/sdks/v2 \
    --go_out=$PKG_MAP:. \
    --go-grpc_out=. \
    $PROTOS

  insert_license_header '-d 1'
}

function gen_beam_model_protos() {
  cd "$PROJECT_ROOT"

  declare -a TO_GEN=(
    'model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/*.proto'
    'model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/*.proto'
    'model/fn-execution/src/main/proto/org/apache/beam/model/fn_execution/v1/*.proto'
  )

  declare -a INCLUDES=()
  for package in "${TO_GEN[@]}"
  do
    INCLUDES+=("-I${package%/org*}")
  done

  for package in "${TO_GEN[@]}"
  do
    protoc \
      "${INCLUDES[@]}" \
      --go_opt=module=github.com/apache/beam/sdks/v2 \
      --go-grpc_opt=module=github.com/apache/beam/sdks/v2 \
      --go_out="$PROJECT_ROOT/sdks" \
      --go-grpc_out="$PROJECT_ROOT/sdks" \
      $package
  done

  insert_license_header
}

if [[ $1 == "model" ]]; then
  gen_beam_model_protos
else
  gen_go_sdk_protos
fi
