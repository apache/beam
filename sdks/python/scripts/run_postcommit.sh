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
# This script will be run by Jenkins as a post commit test. In order to run
# locally make the following changes:
#
# GCS_LOCATION -> Temporary location to use for service tests.
# PROJECT      -> Project name to use for service jobs.
#


###########################################################################
# Usage check.

if (( $# < 2 )); then
  printf "Usage: \n$> ./scripts/run_postcommit.sh <test_type> <pipeline_type> <runner_type> "
  printf  " [gcp_location] [gcp_project] [dataflow_worker_jar]"
  printf "\n\ttest_type: [required] ValidatesRunner or IT"
  printf "\n\tpipeline_type: [required] streaming or batch"
  printf "\n\trunner_type: [optional] TestDataflowRunner or TestDirectRunner"
  printf "\n\tgcp_location: [optional] A gs:// path to stage artifacts and output results"
  printf "\n\tgcp_project: [optional] A GCP project to run Dataflow pipelines"
  printf "\n\tdataflow_worker_jar: [optional] Customized worker jar for dataflow runner.\n"
  exit 1
fi

set -e
set -v


###########################################################################
# Build tarball and set pipeline options.

# Check that the script is running in a known directory.
if [[ $PWD != *sdks/python* ]]; then
  echo 'Unable to locate Apache Beam Python SDK root directory'
  exit 1
fi

# Go to the Apache Beam Python SDK root
if [[ "*sdks/python" != $PWD ]]; then
  cd $(pwd | sed 's/sdks\/python.*/sdks\/python/')
fi

RUNNER=${3:-TestDataflowRunner}

# Where to store integration test outputs.
GCS_LOCATION=${4:-gs://temp-storage-for-end-to-end-tests}

PROJECT=${5:-apache-beam-testing}

# Path to customized worker jar file.
DATAFLOW_WORKER_JAR=${6:-}

# Create a tarball
python setup.py -q sdist

SDK_LOCATION=$(find dist/apache-beam-*.tar.gz)

# Install test dependencies for ValidatesRunner tests.
echo "pyhamcrest" > postcommit_requirements.txt
echo "mock" >> postcommit_requirements.txt

# Options used to run testing pipeline on Cloud Dataflow Service. Also used for
# running on DirectRunner (some options ignored).
PIPELINE_OPTIONS=(
  "--runner=$RUNNER"
  "--project=$PROJECT"
  "--staging_location=$GCS_LOCATION/staging-it"
  "--temp_location=$GCS_LOCATION/temp-it"
  "--output=$GCS_LOCATION/py-it-cloud/output"
  "--sdk_location=$SDK_LOCATION"
  "--requirements_file=postcommit_requirements.txt"
  "--num_workers=1"
  "--sleep_secs=20"
)

# Validation: For testing purpose, we require to use the optional feature of dataflow worker
# jar file, when streaming and TestDataFlow are both specified.
if [ "$2" = "streaming" ] && [ "$3" = "TestDataflowRunner" ] && [ -z $DATAFLOW_WORKER_JAR]; then
  echo "Failure: Testing pipelines for streaming expect a dataflow worker jar file."
  exit 1
fi

# Add streaming flag if specified.
if [[ "$2" = "streaming" ]]; then
  echo ">>> Set test pipeline to streaming"
  PIPELINE_OPTIONS+=("--streaming")
  if [ "$3" = "TestDataflowRunner" ]; then
    PIPELINE_OPTIONS+=("--dataflow_worker_jar $DATAFLOW_WORKER_JAR")
  fi
else
  echo ">>> Set test pipeline to batch"
fi

TESTS=""
if [[ "$3" = "TestDirectRunner" ]]; then
  if [[ "$2" = "streaming" ]]; then
    TESTS="--tests=\
apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it,\
apache_beam.io.gcp.pubsub_integration_test:PubSubIntegrationTest"
  else
    TESTS="--tests=\
apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it,\
apache_beam.io.gcp.pubsub_integration_test:PubSubIntegrationTest,\
apache_beam.io.gcp.big_query_query_to_table_it_test:BigQueryQueryToTableIT,\
apache_beam.io.gcp.bigquery_io_read_it_test"
  fi
fi

###########################################################################
# Run tests and validate that jobs finish successfully.

JOINED_OPTS=$(IFS=" " ; echo "${PIPELINE_OPTIONS[*]}")

echo ">>> RUNNING $RUNNER $1 tests"
python setup.py nosetests \
  --attr $1 \
  --nologcapture \
  --processes=8 \
  --process-timeout=3000 \
  --test-pipeline-options="$JOINED_OPTS" \
  $TESTS
