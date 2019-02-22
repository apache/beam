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

# This script validates python 3 container. It will be run by Jenkins as a post commit test.
# In order to run locally make the following changes:
#
# LOCAL_PATH   -> Path of tox and virtualenv if you have them already installed.
# GCS_LOCATION -> Temporary location to use for service tests.
# PROJECT      -> Project name to use for dataflow and docker images.
#
# Execute from the root of the repository: sdks/python/container/run_validatescontainer.sh

echo "This script must be executed in the root of beam project. Please set LOCAL_PATH, GCS_LOCATION, and PROJECT as desired."

set -e
set -v

# Where to store integration test outputs.
GCS_LOCATION=${GCS_LOCATION:-gs://temp-storage-for-end-to-end-tests}

# Project for the container and integration test
PROJECT=${PROJECT:-apache-beam-testing}

# Verify in the root of the repository
test -d sdks/python/container

# Verify docker and gcloud commands exist
command -v docker
command -v gcloud
docker -v
gcloud -v

# Build the container image
TAG=$(date +%Y%m%d-%H%M%S)
CONTAINER=us.gcr.io/$PROJECT/$USER/python3
echo "Building Python 3 container $CONTAINER"
./gradlew :beam-sdks-python-container-py3:docker -Pdocker-repository-root=us.gcr.io/$PROJECT/$USER -Pdocker-tag=$TAG -Ppython3 --info

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

echo ">>> Successfully built Python 3 container $CONTAINER"

# Python 3 virtualenv for the rest of the script to run setup & e2e test
virtualenv sdks/python/container/py3/venv -p python3.5
. sdks/python/container/py3/venv/bin/activate
cd sdks/python
pip install -e .[gcp,test]

# Install test dependencies for ValidatesContainer tests.
echo "pyhamcrest" > postcommit_requirements.txt

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
  --test-pipeline-options=" \
    --runner=TestDataflowRunner \
    --project=$PROJECT \
    --worker_harness_container_image=$CONTAINER:$TAG \
    --staging_location=$GCS_LOCATION/staging-validatesrunner-test \
    --temp_location=$GCS_LOCATION/temp-validatesrunner-test \
    --output=$GCS_LOCATION/output \
    --sdk_location=$SDK_LOCATION \
    --requirements_file=postcommit_requirements.txt \
    --num_workers=1"

echo ">>> SUCCESS DATAFLOW RUNNER VALIDATESCONTAINER TEST"
