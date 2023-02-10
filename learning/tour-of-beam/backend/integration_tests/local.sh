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

# demo- prefix makes firebase emulator thinking we're in a local-only environment
export GOOGLE_CLOUD_PROJECT=demo-test-proj
export FIREBASE_AUTH_EMULATOR_HOST=localhost:9099

# Enable TOB_MOCK to mock out datastore
#export TOB_MOCK=1
export DATASTORE_PROJECT_ID=$GOOGLE_CLOUD_PROJECT
export DATASTORE_EMULATOR_HOST=localhost:8081
export DATASTORE_EMULATOR_DATADIR=./datadir-$(date '+%H-%M-%S')
export PLAYGROUND_ROUTER_HOST=localhost:8000

export TOB_LEARNING_ROOT=./samples/learning-content

export PORT_SDK_LIST=8801
export PORT_GET_CONTENT_TREE=8802
export PORT_GET_UNIT_CONTENT=8803
export PORT_GET_USER_PROGRESS=8804
export PORT_POST_UNIT_COMPLETE=8805
export PORT_POST_USER_CODE=8806
export PORT_POST_DELETE_PROGRESS=8807

mkdir "$DATASTORE_EMULATOR_DATADIR"

docker-compose up -d

go build -o tob_function cmd/main.go

PORT=$PORT_SDK_LIST FUNCTION_TARGET=getSdkList         ./tob_function &
PORT=$PORT_GET_CONTENT_TREE FUNCTION_TARGET=getContentTree  ./tob_function &
PORT=$PORT_GET_UNIT_CONTENT FUNCTION_TARGET=getUnitContent  ./tob_function &
PORT=$PORT_GET_USER_PROGRESS FUNCTION_TARGET=getUserProgress ./tob_function &
PORT=$PORT_POST_UNIT_COMPLETE FUNCTION_TARGET=postUnitComplete ./tob_function &
PORT=$PORT_POST_USER_CODE FUNCTION_TARGET=postUserCode ./tob_function &
PORT=$PORT_POST_DELETE_PROGRESS FUNCTION_TARGET=postDeleteProgress ./tob_function &

sleep 5


go run cmd/ci_cd/ci_cd.go

# -count=1 is an idiomatic way to disable test caching
go test -v -count=1 --tags integration ./integration_tests/...

pkill -P $$

rm -f ./tob_function

docker-compose down


ls "$DATASTORE_EMULATOR_DATADIR"
cat "$DATASTORE_EMULATOR_DATADIR/WEB-INF/index.yaml"

diff "$DATASTORE_EMULATOR_DATADIR/WEB-INF/index.yaml" internal/storage/index.yaml || ( echo "index.yaml mismatch"; exit 1)


rm -rf "$DATASTORE_EMULATOR_DATADIR"
