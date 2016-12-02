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

# Remove any tox cache from previous workspace
rm -rf sdks/python/.tox

# INFRA does not install virtualenv
pip install virtualenv --user

# INFRA does not install tox
pip install tox --user

# Tox runs unit tests in a virtual environment
${LOCAL_PATH}/tox -e py27 -c sdks/python/tox.ini

# Virtualenv for the rest of the script to run setup & e2e tests
${LOCAL_PATH}/virtualenv sdks/python
. sdks/python/bin/activate
cd sdks/python
python setup.py install

# Run wordcount in the Direct Runner and validate output.
echo ">>> RUNNING DIRECT RUNNER py-wordcount"
python -m apache_beam.examples.wordcount --output /tmp/py-wordcount-direct
# TODO: check that output file is generated for Direct Runner.

# Run tests on the service.

# Where to store wordcount output.
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests

# Job name needs to be unique
JOBNAME_E2E=py-wordcount-`date +%s`
JOBNAME_VR_TEST=py-validatesrunner-`date +%s`

PROJECT=apache-beam-testing

# Create a tarball
python setup.py sdist

# Run ValidatesRunner tests on Google Cloud Dataflow service
python setup.py nosetests \
  --a ValidatesRunner --processes=4 --process-timeout=360 \
  --test-pipeline-options=\
    "--runner=BlockingDataflowPipelineRunner \
    --project=$PROJECT \
    --staging_location=$GCS_LOCATION/staging-validatesrunner-test \
    --sdk_location=dist/apache-beam-sdk-*.tar.gz \
    --job_name=$JOBNAME_VR_TEST\
    --num_workers=1"

# Run wordcount on the Google Cloud Dataflow service
python -m apache_beam.examples.wordcount \
  --output $GCS_LOCATION/py-wordcount-cloud \
  --staging_location $GCS_LOCATION/staging-wordcount \
  --temp_location $GCS_LOCATION/temp-wordcount \
  --runner BlockingDataflowPipelineRunner \
  --job_name $JOBNAME_E2E \
  --project $PROJECT \
  --sdk_location dist/apache-beam-sdk-*.tar.gz \
  --num_workers 1 >> job_output 2>&1 || true;

# Print full job output, validate correct, then remove it.
echo ">>> JOB OUTPUT FOLLOWS"
cat job_output

# Grep will exit with status 1 if success message was not found.
echo ">>> CHECKING JOB SUCCESS"
grep JOB_STATE_DONE job_output

# Remove the job output.
rm job_output
