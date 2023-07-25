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
# GCS_LOCATION -> Temporary location to use for service tests.
# PROJECT      -> Project name to use for dataflow and docker images.
# REGION       -> Region name to use for Dataflow
#
# Execute from the root of the repository:
#     test Python3.8 container:
#         ./gradlew :sdks:python:test-suites:dataflow:py38:validatesContainer
#     or test all supported python versions together:
#         ./gradlew :sdks:python:test-suites:dataflow:validatesContainer

echo "This script must be executed in the root of beam project. Please set GCS_LOCATION, PROJECT and REGION as desired."

if [[ $# != 2 ]]; then
  printf "Usage: \n$> ./sdks/python/container/run_validatescontainer.sh <python_version> <sdk_location>"
  printf "\n\tpython_version: [required] Python version used for container build and run tests."
  printf " Sample value: 3.9"
  exit 1
fi

set -e
set -v

# Where to store integration test outputs.
GCS_LOCATION=${GCS_LOCATION:-gs://temp-storage-for-end-to-end-tests}

# Project for the container and integration test
PROJECT=${PROJECT:-apache-beam-testing}
REGION=${REGION:-us-central1}
IMAGE_PREFIX="$(grep 'docker_image_default_repo_prefix' gradle.properties | cut -d'=' -f2)"
SDK_VERSION="$(grep 'sdk_version' gradle.properties | cut -d'=' -f2)"
PY_VERSION=$1
IMAGE_NAME="${IMAGE_PREFIX}python${PY_VERSION}_sdk"
CONTAINER_PROJECT="sdks:python:container:py${PY_VERSION//.}"  # Note: we substitute away the dot in the version.
PY_INTERPRETER="python${PY_VERSION}"

XUNIT_FILE="pytest-$IMAGE_NAME.xml"

# Verify in the root of the repository
test -d sdks/python/container

# Verify docker and gcloud commands exist
command -v docker
command -v gcloud
docker -v
gcloud -v

# Verify docker image has been built.
docker images | grep "apache/$IMAGE_NAME" | grep "$SDK_VERSION"

TAG=$(date +%Y%m%d-%H%M%S%N)
CONTAINER=us.gcr.io/$PROJECT/$USER/$IMAGE_NAME
PREBUILD_SDK_CONTAINER_REGISTRY_PATH=us.gcr.io/$PROJECT/$USER/prebuild_python${PY_VERSION//.}_sdk
echo "Using container $CONTAINER"

# Tag the docker container.
docker tag "apache/$IMAGE_NAME:$SDK_VERSION" "$CONTAINER:$TAG"

# Push the container.
gcloud docker -- push $CONTAINER:$TAG

function cleanup_container {
  # Delete the container locally and remotely
  docker rmi $CONTAINER:$TAG || echo "Failed to remove container image"
  for image in $(docker images --format '{{.Repository}}:{{.Tag}}' | grep $PREBUILD_SDK_CONTAINER_REGISTRY_PATH)
    do docker rmi $image || echo "Failed to remove prebuilt sdk container image"
  done
  gcloud --quiet container images delete $CONTAINER:$TAG || echo "Failed to delete container"
  for digest in $(gcloud container images list-tags $PREBUILD_SDK_CONTAINER_REGISTRY_PATH/beam_python_prebuilt_sdk  --format="get(digest)")
    do gcloud container images delete $PREBUILD_SDK_CONTAINER_REGISTRY_PATH/beam_python_prebuilt_sdk@$digest --force-delete-tags --quiet || echo "Failed to remove prebuilt sdk container image"
  done

  echo "Removed the container"
}
trap cleanup_container EXIT

echo ">>> Successfully built and push container $CONTAINER"

cd sdks/python
SDK_LOCATION=$2

# Run ValidatesRunner tests on Google Cloud Dataflow service
echo ">>> RUNNING DATAFLOW RUNNER VALIDATESCONTAINER TEST"
pytest -o junit_suite_name=$IMAGE_NAME \
  -m="it_validatescontainer" \
  --show-capture=no \
  --numprocesses=1 \
  --timeout=1800 \
  --junitxml=$XUNIT_FILE \
  --ignore-glob '.*py3\d?\.py$' \
  --log-cli-level=INFO \
  --test-pipeline-options=" \
    --runner=TestDataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --sdk_container_image=$CONTAINER:$TAG \
    --staging_location=$GCS_LOCATION/staging-validatesrunner-test \
    --temp_location=$GCS_LOCATION/temp-validatesrunner-test \
    --output=$GCS_LOCATION/output \
    --sdk_location=$SDK_LOCATION \
    --num_workers=1 \
    --docker_registry_push_url=$PREBUILD_SDK_CONTAINER_REGISTRY_PATH"

echo ">>> SUCCESS DATAFLOW RUNNER VALIDATESCONTAINER TEST"
