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
# PROJECT      -> Project name to use for service jobs.
#
# Execute from the root of the repository: sdks/python/run_postcommit.sh

set -e
set -v

# pip install --user installation location.
LOCAL_PATH=$HOME/.local/bin/

# INFRA does not install virtualenv
pip install virtualenv --user

# Virtualenv for the rest of the script to run setup & e2e tests
${LOCAL_PATH}/virtualenv sdks/python
. sdks/python/bin/activate
cd sdks/python
pip install -e .[gcp,test]

# Where to store integration test outputs.
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests

PROJECT=apache-beam-testing

# Create a tarball
python setup.py sdist

SDK_LOCATION=$(find dist/apache-beam-*.tar.gz)

# Install test dependencies for ValidatesRunner tests.
echo "pyhamcrest" > postcommit_requirements.txt
echo "mock" >> postcommit_requirements.txt

# Run ValidatesRunner tests on Google Cloud Dataflow service
echo ">>> RUNNING DATAFLOW RUNNER VALIDATESRUNNER TESTS"
python setup.py nosetests \
  --attr ValidatesRunner \
  --nocapture \
  --processes=4 \
  --process-timeout=900 \
  --test-pipeline-options=" \
    --runner=TestDataflowRunner \
    --project=$PROJECT \
    --staging_location=$GCS_LOCATION/staging-validatesrunner-test \
    --temp_location=$GCS_LOCATION/temp-validatesrunner-test \
    --sdk_location=$SDK_LOCATION \
    --requirements_file=postcommit_requirements.txt \
    --num_workers=1"
