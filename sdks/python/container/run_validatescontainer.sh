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
#     test Python3.8 x86 container:
#         ./gradlew :sdks:python:test-suites:dataflow:py38:validatesContainer
#     or test all supported python versions x86 containers together:
#         ./gradlew :sdks:python:test-suites:dataflow:validatesContainer
#
# Note: ARM test suites only run on github actions. For example, to test Python3.8 ARM containers,
# commenting `Run Python ValidatesContainer Dataflow ARM (3.8)` will trigger the test.

echo "This script must be executed in the root of beam project. Please set GCS_LOCATION, PROJECT and REGION as desired."

if [[ $# < 2 ]]; then
  printf "Usage: \n$> ./sdks/python/container/run_validatescontainer.sh <python_version> <sdk_location> <cpu_architecture>"
  printf "\n\tpython_version: [required] Python version used for container build and run tests."
  printf " Sample value: 3.9"
  printf "\n\tcpu_architecture: [optional] CPU architecture used for container build and run tests, default as x86."
  printf " Sample value: ARM or x86"
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
ARCH=${3:-"x86"}
IMAGE_SUFFIX=${IMAGE_SUFFIX:-_sdk}
IMAGE_NAME="${IMAGE_PREFIX}python${PY_VERSION}${IMAGE_SUFFIX}"
CONTAINER_PROJECT="sdks:python:container:py${PY_VERSION//.}"  # Note: we substitute away the dot in the version.
PY_INTERPRETER="python${PY_VERSION}"
MACHINE_TYPE_ARGS=""

XUNIT_FILE="pytest-$IMAGE_NAME.xml"

# Verify in the root of the repository
test -d sdks/python/container

# Verify docker and gcloud commands exist
command -v docker
command -v gcloud
docker -v
gcloud -v

TAG=$(date +%Y%m%d-%H%M%S%N)
CONTAINER=us.gcr.io/$PROJECT/$USER/$IMAGE_NAME
PREBUILD_SDK_CONTAINER_REGISTRY_PATH=us.gcr.io/$PROJECT/$USER/prebuild_python${PY_VERSION//.}_sdk
echo "Using container $CONTAINER"
echo "Using CPU architecture $ARCH"

if [[ "$ARCH" == "x86" ]]; then
  # Verify docker image has been built.
  docker images | grep "apache/$IMAGE_NAME" | grep "$SDK_VERSION"

  # Tag the docker container.
  docker tag "apache/$IMAGE_NAME:$SDK_VERSION" "$CONTAINER:$TAG"

  # Push the container
  gcloud docker -- push $CONTAINER:$TAG
elif [[ "$ARCH" == "ARM" ]]; then
  # Reset the multi-arch Python SDK container image tag.
  TAG=$MULTIARCH_TAG
  MACHINE_TYPE_ARGS="--machine_type=t2a-standard-1"
else
  printf "Please give a valid CPU architecture, either x86 or ARM."
  exit 1
fi

function cleanup_container {
  # Delete the container locally and remotely
  docker rmi $CONTAINER:$TAG || echo "Built container image was not removed. Possibly, it was not not saved locally."
  for image in $(docker images --format '{{.Repository}}:{{.Tag}}' | grep $PREBUILD_SDK_CONTAINER_REGISTRY_PATH)
    do docker rmi $image || echo "Failed to remove prebuilt sdk container image"
  done
  # Note: we don't delete the multi-arch containers here because this command only deletes the manifest list with the tag,
  # the associated container images can't be deleted because they are not tagged. However, multi-arch containers that are
  # older than 6 weeks old are deleted by stale_dataflow_prebuilt_image_cleaner.sh that runs daily.
  if [[ "$ARCH" == "x86" ]]; then
    gcloud --quiet container images delete $CONTAINER:$TAG || echo "Failed to delete container"
  fi
  for digest in $(gcloud container images list-tags $PREBUILD_SDK_CONTAINER_REGISTRY_PATH/beam_python_prebuilt_sdk  --format="get(digest)")
    do gcloud container images delete $PREBUILD_SDK_CONTAINER_REGISTRY_PATH/beam_python_prebuilt_sdk@$digest --force-delete-tags --quiet || echo "Failed to remove prebuilt sdk container image"
  done

  echo "Removed the container"
}
trap cleanup_container EXIT

echo ">>> Successfully built and push container $CONTAINER"

cd sdks/python
SDK_LOCATION=$2

echo ">>> RUNNING DATAFLOW RUNNER VALIDATESCONTAINER TEST"
pytest -o log_cli=True -o log_level=Info -o junit_suite_name=$IMAGE_NAME \
  -m=it_validatescontainer \
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
    $MACHINE_TYPE_ARGS \
    --docker_registry_push_url=$PREBUILD_SDK_CONTAINER_REGISTRY_PATH"

echo ">>> SUCCESS DATAFLOW RUNNER VALIDATESCONTAINER TEST"
