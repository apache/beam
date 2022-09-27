#!/bin/bash -eux

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export DATASTORE_PROJECT_ID=test-proj
export DATASTORE_EMULATOR_HOST=localhost:8081
export DATASTORE_EMULATOR_DATADIR=./datadir-$(date '+%H-%M-%S')
export TOB_LEARNING_ROOT=./samples/learning-content

export PORT_SDK_LIST=8801
export PORT_GET_CONTENT_TREE=8802
export PORT_GET_UNIT_CONTENT=8803

mkdir "$DATASTORE_EMULATOR_DATADIR"

docker-compose up -d

go build -o tob_function cmd/main.go

PORT=$PORT_SDK_LIST FUNCTION_TARGET=getSdkList         ./tob_function &
PORT=$PORT_GET_CONTENT_TREE FUNCTION_TARGET=getContentTree  ./tob_function &
PORT=$PORT_GET_UNIT_CONTENT FUNCTION_TARGET=getUnitContent  ./tob_function &

sleep 5


go run cmd/ci_cd/ci_cd.go


go test -v --tags integration ./integration_tests/...

pkill -P $$

rm -f ./tob_function

docker-compose down


ls "$DATASTORE_EMULATOR_DATADIR"
cat "$DATASTORE_EMULATOR_DATADIR/WEB-INF/index.yaml"

diff "$DATASTORE_EMULATOR_DATADIR/WEB-INF/index.yaml" internal/storage/index.yaml || ( echo "index.yaml mismatch"; exit 1)


rm -rf "$DATASTORE_EMULATOR_DATADIR"
