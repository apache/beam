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

###########################################################################
#
# This script is useful to run single or a set of Python integration tests
# manually or through Gradle. Note, this script doesn't setup python
# environment which is required before running tests. Use Gradle task
# `beam-sdks-python:integrationTests` to do both together.
#
# In order to run test with customer options, use following commandline flags:
#
# Pipeline related flags:
#     runner        -> Runner that execute pipeline job.
#                      e.g. TestDataflowRunner, DirectRunner
#     project       -> Project name of the cloud service.
#     gcs_location  -> Base location on GCS. Some pipeline options are
#                      dirived from it including output, staging_location
#                      and temp_location.
#     sdk_location  -> Python tar ball location. Glob is accepted.
#     num_workers   -> Number of workers.
#     sleep_secs    -> Number of seconds to wait before verification.
#     pipeline_opts -> List of space separateed pipeline options. If this
#                      flag is specified, all above flag will be ignored.
#                      Please include all required pipeline options when
#                      using this flag.
#
# Test related flags:
#     test_opts     -> List of space separated options to configure Nose test
#                      during execution. Commonly used options like `--attr`,
#                      `--tests`, `--nologcapture`. More can be found in
#                      https://nose.readthedocs.io/en/latest/man.html#options
#
# Example usages:
#     - Run full set of PostCommit tests with default pipeline options:
#     `$ ./run_integration_test.sh`
#
#     - Run single integration test with default pipeline options:
#     `$ ./run_integration_test.sh --test_opts --tests=apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it`
#
#     - Run full set of PostCommit tests with customized pipeline options:
#     `$ ./run_integration_test.sh --project my-project --gcs_location gs://my-location`

###########################################################################
# Get pipeline options specified from commandline arguments.

# Default pipeline options
PROJECT=apache-beam-testing
RUNNER=TestDataflowRunner
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests
SDK_LOCATION=dist/apache-beam-*.tar.gz
NUM_WORKERS=1
SLEEP_SECS=20

# Default test (nose) options.
# Default test sets are full integration tests.
TEST_OPTS="--attr=IT --nocapture"

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --runner)
        RUNNER="$2"
        shift # past argument
        shift # past value
        ;;
    --project)
        PROJECT="$2"
        shift # past argument
        shift # past value
        ;;
    --gcs_location)
        GCS_LOCATION="$2"
        shift # past argument
        shift # past value
        ;;
    --sdk_location)
        SDK_LOCATION="$2"
        shift # past argument
        shift # past value
        ;;
    --num_workers)
        NUM_WORKERS="$2"
        shift # past argument
        shift # past value
        ;;
    --sleep_secs)
        SLEEP_SECS="$2"
        shift # past argument
        shift # past value
        ;;
    --pipeline_opts)
        PIPELINE_OPTS="$2"
        shift # past argument
        shift # past value
        ;;
    --test_opts)
        TEST_OPTS="$2"
        shift # past argument
        shift # past value
        ;;
    *)    # unknown option
        echo "Unknown option: $1"
        exit 1
        ;;
esac
done

set -o errexit
set -o verbose


###########################################################################
# Build pipeline options if not provided in --pipeline_opts from commandline

if [[ -z $PIPELINE_OPTS ]]; then

  # Check that the script is running in a known directory.
  if [[ $PWD != *sdks/python* ]]; then
    echo 'Unable to locate Apache Beam Python SDK root directory'
    exit 1
  fi

  # Go to the Apache Beam Python SDK root
  if [[ "*sdks/python" != $PWD ]]; then
    cd $(pwd | sed 's/sdks\/python.*/sdks\/python/')
  fi

  # Create a tarball if not exists
  SDK_LOCATION=$(find ${SDK_LOCATION})
  if [[ ! -f $SDK_LOCATION ]]; then
    python setup.py -q sdist
    SDK_LOCATION=$(find dist/apache-beam-*.tar.gz)
  fi

  # Install test dependencies for ValidatesRunner tests.
  echo "pyhamcrest" > postcommit_requirements.txt
  echo "mock" >> postcommit_requirements.txt

  # Options used to run testing pipeline on Cloud Dataflow Service. Also used for
  # running on DirectRunner (some options ignored).
  opts=(
    "--runner=$RUNNER"
    "--project=$PROJECT"
    "--staging_location=$GCS_LOCATION/staging-it"
    "--temp_location=$GCS_LOCATION/temp-it"
    "--output=$GCS_LOCATION/py-it-cloud/output"
    "--sdk_location=$SDK_LOCATION"
    "--requirements_file=postcommit_requirements.txt"
    "--num_workers=$NUM_WORKERS"
    "--sleep_secs=$SLEEP_SECS"
  )
  PIPELINE_OPTS=$(IFS=" " ; echo "${opts[*]}")

fi


###########################################################################
# Run tests and validate that jobs finish successfully.

echo ">>> RUNNING integration tests with pipeline options: $PIPELINE_OPTS"
python setup.py nosetests \
  --test-pipeline-options="$PIPELINE_OPTS" \
  $TEST_OPTS
