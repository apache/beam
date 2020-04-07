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
#     worker_jar    -> Customized worker jar for dataflow runner.
#     kms_key_name  -> Name of Cloud KMS encryption key to use in some tests.
#     pipeline_opts -> List of space separated pipeline options. If this
#                      flag is specified, all above flag will be ignored.
#                      Please include all required pipeline options when
#                      using this flag.
#
# Test related flags:
#     test_opts     -> List of space separated options to configure Nose test
#                      during execution. Commonly used options like `--attr`,
#                      `--tests`, `--nologcapture`. More can be found in
#                      https://nose.readthedocs.io/en/latest/man.html#options
#     suite         -> Namespace for this run of tests. Required if running
#                      under Jenkins. Used to differentiate runs of the same
#                      tests with different interpreters/dependencies/etc.
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
REGION=us-central1
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests
SDK_LOCATION=build/apache-beam.tar.gz
NUM_WORKERS=1
SLEEP_SECS=20
STREAMING=false
WORKER_JAR=""
KMS_KEY_NAME="projects/apache-beam-testing/locations/global/keyRings/beam-it/cryptoKeys/test"
SUITE=""

# Default test (nose) options.
# Run WordCountIT.test_wordcount_it by default if no test options are
# provided.
TEST_OPTS="--tests=apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it --nocapture"

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
    --worker_jar)
        WORKER_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --runner_v2)
        RUNNER_V2="$2"
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
XUNIT_FILE="nosetests-$SUITE.xml"

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

  # Install test dependencies for ValidatesRunner tests.
  # pyhamcrest==1.10.0 doesn't work on Py2.
  # See: https://github.com/hamcrest/PyHamcrest/issues/131.
  echo "pyhamcrest!=1.10.0,<2.0.0" > postcommit_requirements.txt
  echo "mock<3.0.0" >> postcommit_requirements.txt
  echo "parameterized>=0.7.1,<0.8.0" >> postcommit_requirements.txt

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
    "--requirements_file=postcommit_requirements.txt"
    "--num_workers=$NUM_WORKERS"
    "--sleep_secs=$SLEEP_SECS"
  )

  # Add --streaming if provided
  if [[ "$STREAMING" = true ]]; then
    opts+=("--streaming")
  fi

  # Add --dataflow_worker_jar if provided
  if [[ ! -z "$WORKER_JAR" ]]; then
    opts+=("--dataflow_worker_jar=$WORKER_JAR")
  fi

  # Add --runner_v2 if provided
  if [[ "$RUNNER_V2" = true ]]; then
    opts+=("--experiments=use_runner_v2")
    if [[ "$STREAMING" = true ]]; then
      # Dataflow Runner V2 only supports streaming engine.
      opts+=("--enable_streaming_engine")
    else
      opts+=("--experiments=beam_fn_api")
    fi

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

###########################################################################
# Run tests and validate that jobs finish successfully.

echo ">>> RUNNING integration tests with pipeline options: $PIPELINE_OPTS"
echo ">>>   test options: $TEST_OPTS"
# TODO(BEAM-3713): Pass $SUITE once migrated to pytest. xunitmp doesn't support
#   suite names.
python setup.py nosetests \
  --test-pipeline-options="$PIPELINE_OPTS" \
  --with-xunitmp --xunitmp-file=$XUNIT_FILE \
  --ignore-files '.*py3\d?\.py$' \
  $TEST_OPTS
