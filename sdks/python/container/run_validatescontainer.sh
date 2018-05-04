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
# LOCAL_PATH   -> Path of tox and virtualenv if you have them already installed.
# GCS_LOCATION -> Temporary location to use for service tests.
# PROJECT      -> Project name to use for dataflow and docker images.
#
# Execute from the root of the repository: sdks/python/container/run_validatescontainer.sh

set -e
set -v

# Where to store integration test outputs.
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests

# Project for the container and integration test
PROJECT=apache-beam-testing

# Verify in the root of the repository
test -d sdks/python/container

# Verify docker and gcloud commands exist
command -v docker
command -v gcloud
docker -v
gcloud -v

# Create a tarball
./gradlew :beam-sdks-python:clean --info
./gradlew :beam-sdks-python:sdist --info
SDK_LOCATION=$(find sdks/python/build/apache-beam-*.tar.gz)

# Build the container
TAG=$(date +%Y%m%d-%H%M%S)
CONTAINER=us.gcr.io/$PROJECT/$USER/python
echo "Using container $CONTAINER"
./gradlew :beam-sdks-python-container:docker -Pdocker-repository-root=us.gcr.io/$PROJECT/$USER -Pdocker-tag=$TAG --info

# Verify it exists
docker images | grep $TAG

# Push the container
gcloud docker -- push $CONTAINER

# Run ValidatesRunner tests on Google Cloud Dataflow service
echo ">>> RUNNING DATAFLOW RUNNER VALIDATESCONTAINER TEST"
. sdks/python/build/gradleenv/bin/activate
python setup.py nosetests \
  --attr ValidatesContainer \
  --nocapture \
  --processes=1 \
  --process-timeout=900 \
  --test-pipeline-options=" \
    --runner=TestDataflowRunner \
    --project=$PROJECT \
    --worker_harness_container_image=$CONTAINER:$TAG \
    --staging_location=$GCS_LOCATION/staging-validatesrunner-test \
    --temp_location=$GCS_LOCATION/temp-validatesrunner-test \
    --output=$GCS_LOCATION/output \
    --sdk_location=$SDK_LOCATION \
    --num_workers=1"

# Delete the container locally and remotely
docker rmi $CONTAINER:$TAG || echo "Failed to remove container"
gcloud --quiet container images delete $CONTAINER:$TAG || echo "Failed to delete container"

echo ">>> SUCCESS DATAFLOW RUNNER VALIDATESCONTAINER TEST"
