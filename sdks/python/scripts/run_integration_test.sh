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
# This script is used in Gradle to run single or a set of Python integration tests
# locally or on Jenkins. Note, this script doesn't setup python environment which is
# required for integration test. In order to do so, run Gradle tasks defined in
# :sdks:python:test-suites instead.
#
# In order to run test with customer options, use following commandline flags:
#
# Pipeline related flags:
#     runner        -> Runner that execute pipeline job.
#                      e.g. TestDataflowRunner, TestDirectRunner
#     project       -> Project name of the cloud service.
#     region        -> Compute Engine region to create the Dataflow job.
#     gcs_location  -> Base location on GCS. Some pipeline options are
#                      derived from it including output, staging_location
#                      and temp_location.
#     sdk_location  -> Python tar ball location. Glob is accepted.
#     num_workers   -> Number of workers.
#     sleep_secs    -> Number of seconds to wait before verification.
#     streaming     -> True if a streaming job.
#     kms_key_name  -> Name of Cloud KMS encryption key to use in some tests.
#     pipeline_opts -> List of space separated pipeline options. If this
#                      flag is specified, all above flag will be ignored.
#                      Please include all required pipeline options when
#                      using this flag.
#
# Test related flags:
#     test_opts     -> List of space separated options to configure Pytest test
#                      during execution. Commonly used options like `--capture=no`
#                      `--collect-only`. More can be found in
#                      https://docs.pytest.org/en/latest/reference.html#command-line-flags
#     suite         -> Namespace for this run of tests. Required if running
#                      under Jenkins. Used to differentiate runs of the same
#                      tests with different interpreters/dependencies/etc.
#
# Example usages:
#     - Run full set of PostCommit tests with default pipeline options:
#     `$ ./run_integration_test.sh`
#
#     - Run single integration test with default pipeline options:
#     `$ ./run_integration_test.sh --test_opts apache_beam/examples/wordcount_it_test.py::WordCountIT::test_wordcount_it`
#
#     - Run full set of PostCommit tests with customized pipeline options:
#     `$ ./run_integration_test.sh --project my-project --gcs_location gs://my-location`

###########################################################################
# Get pipeline options specified from commandline arguments.

# Default pipeline options
PROJECT=apache-beam-testing
RUNNER=TestDataflowRunner
REGION=us-central1
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests
SDK_LOCATION=build/apache-beam.tar.gz
NUM_WORKERS=1
SLEEP_SECS=20
STREAMING=false
KMS_KEY_NAME="projects/apache-beam-testing/locations/global/keyRings/beam-it/cryptoKeys/test"
SUITE=""
COLLECT_MARKERS=
REQUIREMENTS_FILE=""
ARCH=""
PY_VERSION=""

# Default test (pytest) options.
# Run WordCountIT.test_wordcount_it by default if no test options are
# provided.
TEST_OPTS="apache_beam/examples/wordcount_it_test.py::WordCountIT::test_wordcount_it"

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
    --region)
        REGION="$2"
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
    --requirements_file)
      REQUIREMENTS_FILE="$2"
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
    --streaming)
        STREAMING="$2"
        shift # past argument
        shift # past value
        ;;
    --kms_key_name)
        KMS_KEY_NAME="$2"
        shift # past argument
        shift # past value
        ;;
    --dataflow_endpoint)
        DATAFLOW_ENDPOINT="$2"
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
    --suite)
        SUITE="$2"
        shift # past argument
        shift # past value
        ;;
    --collect)
      COLLECT_MARKERS="-m=$2"
      shift # past argument
      shift # past value
      ;;
    --arch)
      ARCH="$2"
      shift # past argument
      shift # past value
      ;;
    --py_version)
      PY_VERSION="$2"
      shift # past argument
      shift # past value
      ;;
    *)    # unknown option
        echo "Unknown option: $1"
        exit 1
        ;;
esac
done

if [[ "$JENKINS_HOME" != "" && "$SUITE" == "" ]]; then
    echo "Argument --suite is required in a Jenkins environment."
    exit 1
fi

set -o errexit


###########################################################################

# Check that the script is running in a known directory.
if [[ $PWD != *sdks/python* ]]; then
  echo 'Unable to locate Apache Beam Python SDK root directory'
  exit 1
fi

# Go to the Apache Beam Python SDK root
if [[ $PWD != *sdks/python ]]; then
  cd $(pwd | sed 's/sdks\/python.*/sdks\/python/')
fi


###########################################################################
# Build pipeline options if not provided in --pipeline_opts from commandline

if [[ -z $PIPELINE_OPTS ]]; then
  # Get tar ball path
  if [[ $(find ${SDK_LOCATION} 2> /dev/null) ]]; then
    SDK_LOCATION=$(find ${SDK_LOCATION} | tail -n1)
  else
    echo "[WARNING] Could not find SDK tarball in SDK_LOCATION: $SDK_LOCATION."
  fi

  # Options used to run testing pipeline on Cloud Dataflow Service. Also used for
  # running on DirectRunner (some options ignored).
  opts=(
    "--runner=$RUNNER"
    "--project=$PROJECT"
    "--region=$REGION"
    "--staging_location=$GCS_LOCATION/staging-it"
    "--temp_location=$GCS_LOCATION/temp-it"
    "--output=$GCS_LOCATION/py-it-cloud/output"
    "--sdk_location=$SDK_LOCATION"
    "--num_workers=$NUM_WORKERS"
    "--sleep_secs=$SLEEP_SECS"
  )

  # Add --streaming if provided
  if [[ "$STREAMING" = true ]]; then
    opts+=("--streaming")
  fi

  if [[ -n "$REQUIREMENTS_FILE" ]]; then
    opts+=("--requirements_file=$REQUIREMENTS_FILE")
  fi

  if [[ "$ARCH" == "ARM" ]]; then
    opts+=("--machine_type=t2a-standard-1")

    IMAGE_NAME="beam_python${PY_VERSION}_sdk"
    opts+=("--sdk_container_image=us.gcr.io/$PROJECT/$USER/$IMAGE_NAME:$MULTIARCH_TAG")
  fi

  if [[ ! -z "$KMS_KEY_NAME" ]]; then
    opts+=(
      "--kms_key_name=$KMS_KEY_NAME"
      "--dataflow_kms_key=$KMS_KEY_NAME"
    )
  fi

  if [[ ! -z "$DATAFLOW_ENDPOINT" ]]; then
    opts+=("--dataflow_endpoint=$DATAFLOW_ENDPOINT")
  fi

  PIPELINE_OPTS=$(IFS=" " ; echo "${opts[*]}")

fi

# Handle double quotes in PIPELINE_OPTS
# add a backslash before `"` to keep it in command line options
PIPELINE_OPTS=${PIPELINE_OPTS//\"/\\\"}

###########################################################################
# Run tests and validate that jobs finish successfully.

echo ">>> RUNNING integration tests with pipeline options: $PIPELINE_OPTS"
echo ">>>   pytest options: $TEST_OPTS"
echo ">>>   collect markers: $COLLECT_MARKERS"
ARGS="-o junit_suite_name=$SUITE -o log_cli=true -o log_level=INFO --junitxml=pytest_$SUITE.xml $TEST_OPTS"
# Handle markers as an independent argument from $TEST_OPTS to prevent errors in space separated flags
if [ -z "$COLLECT_MARKERS" ]; then
  pytest $ARGS --test-pipeline-options="$PIPELINE_OPTS"
else
  pytest $ARGS --test-pipeline-options="$PIPELINE_OPTS" "$COLLECT_MARKERS"
fi
