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
#     test Python2 container: ./sdks/python/container/run_validatescontainer.sh python2
#     test Python3 container: ./sdks/python/container/run_validatescontainer.sh python35
#     test Python3 container: ./sdks/python/container/run_validatescontainer.sh python36
#     test Python3 container: ./sdks/python/container/run_validatescontainer.sh python37
#     test Python3 container: ./sdks/python/container/run_validatescontainer.sh python38

echo "This script must be executed in the root of beam project. Please set GCS_LOCATION, PROJECT and REGION as desired."

if [[ $# != 1 ]]; then
  printf "Usage: \n$> ./sdks/python/container/run_validatescontainer.sh <python_version>"
  printf "\n\tpython_version: [required] Python version used for container build and run tests."
  printf " Use 'python2' for Python2, 'python35' for Python3.5, python36 for Python3.6, python37 for Python3.7, python38 for Python3.8."
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

# Other variables branched by Python version.
if [[ $1 == "python2" ]]; then
  IMAGE_NAME="${IMAGE_PREFIX}python2.7_sdk"    # Use this to create CONTAINER_IMAGE variable.
  CONTAINER_PROJECT="sdks:python:container:py2"  # Use this to build container by Gradle.
  PY_INTERPRETER="python"   # Use this in virtualenv command.
elif [[ $1 == "python35" ]]; then
  IMAGE_NAME="${IMAGE_PREFIX}python3.5_sdk"    # Use this to create CONTAINER_IMAGE variable.
  CONTAINER_PROJECT="sdks:python:container:py35"  # Use this to build container by Gradle.
  PY_INTERPRETER="python3.5"    # Use this in virtualenv command.
elif [[ $1 == "python36" ]]; then
  IMAGE_NAME="${IMAGE_PREFIX}python3.6_sdk"    # Use this to create CONTAINER_IMAGE variable.
  CONTAINER_PROJECT="sdks:python:container:py36"  # Use this to build container by Gradle.
  PY_INTERPRETER="python3.6"    # Use this in virtualenv command.
elif [[ $1 == "python37" ]]; then
  IMAGE_NAME="${IMAGE_PREFIX}python3.7_sdk"    # Use this to create CONTAINER_IMAGE variable.
  CONTAINER_PROJECT="sdks:python:container:py37"  # Use this to build container by Gradle.
  PY_INTERPRETER="python3.7"    # Use this in virtualenv command.
elif [[ $1 == "python38" ]]; then
  IMAGE_NAME="${IMAGE_PREFIX}python3.8_sdk"    # Use this to create CONTAINER_IMAGE variable.
  CONTAINER_PROJECT="sdks:python:container:py38"  # Use this to build container by Gradle.
  PY_INTERPRETER="python3.8"    # Use this in virtualenv command.
else
  echo "Must set Python version with one of 'python2', 'python35', 'python36', 'python37' and 'python38' from commandline."
  exit 1
fi
XUNIT_FILE="nosetests-$IMAGE_NAME.xml"

# Verify in the root of the repository
test -d sdks/python/container

# Verify docker and gcloud commands exist
command -v docker
command -v gcloud
docker -v
gcloud -v

# Build the container
TAG=$(date +%Y%m%d-%H%M%S)
CONTAINER=us.gcr.io/$PROJECT/$USER/$IMAGE_NAME
echo "Using container $CONTAINER"
./gradlew :$CONTAINER_PROJECT:docker -Pdocker-repository-root=us.gcr.io/$PROJECT/$USER -Pdocker-tag=$TAG --info

# Verify it exists
docker images | grep $TAG

# Push the container
gcloud docker -- push $CONTAINER

function cleanup_container {
  # Delete the container locally and remotely
  docker rmi $CONTAINER:$TAG || echo "Failed to remove container"
  gcloud --quiet container images delete $CONTAINER:$TAG || echo "Failed to delete container"
  echo "Removed the container"
}
trap cleanup_container EXIT

echo ">>> Successfully built and push container $CONTAINER"

# Virtualenv for the rest of the script to run setup & e2e test
VENV_PATH=sdks/python/container/venv/$PY_INTERPRETER
virtualenv $VENV_PATH -p $PY_INTERPRETER
. $VENV_PATH/bin/activate
cd sdks/python
pip install -e .[gcp,test]

# Create a tarball
python setup.py sdist
SDK_LOCATION=$(find dist/apache-beam-*.tar.gz)

# Run ValidatesRunner tests on Google Cloud Dataflow service
echo ">>> RUNNING DATAFLOW RUNNER VALIDATESCONTAINER TEST"
python setup.py nosetests \
  --attr ValidatesContainer \
  --nologcapture \
  --processes=1 \
  --process-timeout=900 \
  --with-xunitmp \
  --xunitmp-file=$XUNIT_FILE \
  --ignore-files '.*py3\d?\.py$' \
  --test-pipeline-options=" \
    --runner=TestDataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --worker_harness_container_image=$CONTAINER:$TAG \
    --staging_location=$GCS_LOCATION/staging-validatesrunner-test \
    --temp_location=$GCS_LOCATION/temp-validatesrunner-test \
    --output=$GCS_LOCATION/output \
    --sdk_location=$SDK_LOCATION \
    --num_workers=1"

echo ">>> SUCCESS DATAFLOW RUNNER VALIDATESCONTAINER TEST"
