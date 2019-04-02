#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

# This script will be run by Jenkins as a post commit test. In order to run
# locally make the following changes:
#
# GCS_LOCATION     -> Temporary location to use for service tests.
# PROJECT          -> Project name to use for docker images.
# DATAFLOW_PROJECT -> Project name to use for dataflow.
#
# Execute from the root of the repository. It assumes binaries are built.

set -e
set -v

# Where to store integration test outputs.
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests

# Project for the container and integration test
PROJECT=apache-beam-testing
DATAFLOW_PROJECT=apache-beam-testing

# Verify in the root of the repository
test -d sdks/go/test

# Verify docker and gcloud commands exist
command -v docker
command -v gcloud
docker -v
gcloud -v

# ensure gcloud is version 186 or above
TMPDIR=$(mktemp -d)
gcloud_ver=$(gcloud -v | head -1 | awk '{print $4}')
if [[ "$gcloud_ver" < "186" ]]
then
  pushd $TMPDIR
  curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-186.0.0-linux-x86_64.tar.gz --output gcloud.tar.gz
  tar xf gcloud.tar.gz
  ./google-cloud-sdk/install.sh --quiet
  . ./google-cloud-sdk/path.bash.inc
  popd
  gcloud components update --quiet || echo 'gcloud components update failed'
  gcloud -v
fi

# Build the container
TAG=$(date +%Y%m%d-%H%M%S)
CONTAINER=us.gcr.io/$PROJECT/$USER/go
echo "Using container $CONTAINER"
./gradlew :sdks:go:container:docker -Pdocker-repository-root=us.gcr.io/$PROJECT/$USER -Pdocker-tag=$TAG

# Verify it exists
docker images | grep $TAG

# Push the container
gcloud docker -- push $CONTAINER

DATAFLOW_WORKER_JAR=$(find ./runners/google-cloud-dataflow-java/worker/build/libs/beam-runners-google-cloud-dataflow-java-fn-api-worker-*.jar)
echo "Using Dataflow worker jar: $DATAFLOW_WORKER_JAR"

echo ">>> RUNNING DATAFLOW INTEGRATION TESTS"
./sdks/go/build/bin/integration \
    --runner=dataflow \
    --project=$DATAFLOW_PROJECT \
    --environment_type=DOCKER \
    --environment_config=$CONTAINER:$TAG \
    --staging_location=$GCS_LOCATION/staging-validatesrunner-test \
    --temp_location=$GCS_LOCATION/temp-validatesrunner-test \
    --worker_binary=./sdks/go/test/build/bin/linux-amd64/worker \
    --dataflow_worker_jar=$DATAFLOW_WORKER_JAR

# TODO(herohde) 5/9/2018: run other runner tests here to reuse the container image?

# Delete the container locally and remotely
docker rmi $CONTAINER:$TAG || echo "Failed to remove container"
gcloud --quiet container images delete $CONTAINER:$TAG || echo "Failed to delete container"

# Clean up tempdir
rm -rf $TMPDIR

echo ">>> SUCCESS"
